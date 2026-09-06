# Discord 直播通知接入与备用轮询系统设计方案

本文档记录针对 **DouyinLiveRecorder** 的新特性架构设计：**通过 Discord 接收直播开播事件作为第一优先级，本地定时轮询作为容灾备用兜底方案（Fallback）**。

当前首期支持平台集中在以下 5 个：
* **Weverse** (`weverse.io`)
* **SOOP** (`sooplive.co.kr`，原 AfreecaTV)
* **Instagram** (`instagram.com`)
* **微博** (`weibo.com`)
* **Berriz** (`berriz.in`)

---

## 目录
1. [系统总体架构设计](#1-系统总体架构设计)
2. [容灾与降级状态机（备用方案）](#2-容灾与降级状态机备用方案)
3. [五大目标平台的 Discord 通知配置与来源方案](#3-五大目标平台的-discord-通知配置与来源方案)
4. [本地配置规则设计（主播与 Discord 消息匹配）](#4-本地配置规则设计主播与-discord-消息匹配)
5. [实施路线图](#5-实施路线图)

---

## 1. 系统总体架构设计

### 1.1 现状痛点
原系统对每个主播采用固定循环（默认 120 秒）通过 HTTP/Spider 轮询直播间接口。
* **痛点 1**：对于 Instagram、Weverse、微博等平台，高频轮询容易触发账号风控、IP 封禁、强制登录验证或滑块验证码。
* **痛点 2**：长时间无直播时，频繁发送无效请求消耗网络与系统资源。

### 1.2 新模式：事件驱动 + 看门狗兜底（Event-Driven First with Watchdog Fallback）
* **主路径（事件驱动）**：
  本地启动一个轻量的 Discord Gateway 客户端（基于 WebSocket），长连接保持在线。当 Discord 频道收到开播消息时，瞬间解析并触发对应主播的录制线程，**实现秒级响应且平日零请求**。
* **备用路径（容灾轮询）**：
  录制线程通过 `threading.Event().wait(timeout=...)` 挂起：
  * **Discord 在线时**：挂起超时设为长周期（如 1800 秒 / 30分钟），仅作为漏推的“看门狗（Watchdog）”兜底检测。
  * **Discord 掉线/未启用时**：自动缩短为高频周期（如 120 秒），无缝降级回传统轮询模式。

```
                    ┌────────────────────────┐
                    │ 5大平台开播通知来源    │
                    │(Bot/Follow/RSSHub/HOOK)│
                    └───────────┬────────────┘
                                │ 发送消息
                                ▼
                    ┌────────────────────────┐
                    │ 免费私有 Discord Server│
                    │    (#live-notify)      │
                    └───────────┬────────────┘
                                │ WebSocket Gateway 推送
                                ▼
┌────────────────────────────────────────────────────────────────────────┐
│ DouyinLiveRecorder (本地程序)                                          │
│                                                                        │
│   ┌────────────────────────────────────────────────────────────────┐   │
│   │ DiscordListener (后台线程)                                      │   │
│   │ 1. 监听 on_message                                             │   │
│   │ 2. 规则匹配 (主播 URL / 关键词 / 房间ID)                        │   │
│   │ 3. 唤醒对应主播的 event.set()                                 │   │
│   └───────────────────────────────┬────────────────────────────────┘   │
│                                   │ set() 唤醒                          │
│                                   ▼                                    │
│   ┌────────────────────────────────────────────────────────────────┐   │
│   │ 主播录制线程 start_record(url)                                 │   │
│   │                                                                │   │
│   │  ┌──────────────┐      超时 (如1800s)     ┌──────────────────┐ │   │
│   │  │ event.wait() │ ───────────────────────►│  备用慢速轮询    │ │   │
│   │  └──────┬───────┘ (看门狗兜底/防漏推)      │  (spider探测)    │ │   │
│   │         │ 立即唤醒 (Discord 通知)         └────────┬─────────┘ │   │
│   │         ▼                                          │ 开播      │   │
│   │  ┌─────────────────────────────────────────────────┴─────────┐ │   │
│   │  │ 真实流校验 (spider.get_stream_data)                       │ │   │
│   │  │ 校验通过 ──► 启动 ffmpeg/下载录制 ──► 录制结束回到等待    │ │   │
│   │  └───────────────────────────────────────────────────────────┘ │   │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 2. 容灾与降级状态机（备用方案）

系统设计有严格的三层防护机制：

| 运行状态 | 触发条件 | 轮询等待间隔行为 | 安全保障说明 |
| :--- | :--- | :--- | :--- |
| **状态 1：Discord 正常** | Gateway 保持长连接 | `wait(timeout=1800)`（30分钟） | 平常完全静默零请求；即便 Bot 偶尔漏推，30分钟内必有一次看门狗兜底。 |
| **状态 2：收到开播通知** | Discord 匹配到对应主播 | 立即被 `set()` 唤醒，中断等待 | 零延迟启动推流校验并开录。 |
| **状态 3：Discord 故障降级**| WebSocket 断开或网络异常 | 自动降级为 `wait(timeout=120)` | 自动退化为原本的高频轮询，保证不错过录制。 |
| **状态 4：防误报机制** | 收到通知但主播实际未播 | 校验无流后清空状态重新挂起 | 不会触发 ffmpeg 空转或崩溃报错。 |

---

## 3. 五大目标平台的 Discord 通知配置与来源方案

由于用户明确首期仅支持 **Weverse, SOOP, Instagram, 微博, Berriz** 5 个平台，下表汇总了这 5 个平台在 Discord 端的**零成本/免费接入配置途径**：

### 3.1 方案总览矩阵

| 平台 | 推荐 Discord 消息源方式 | 难度 | 免费程度 | 特性与稳定性 |
| :--- | :--- | :---: | :---: | :--- |
| **Weverse** | 方式 A：加入大型粉丝/官方服并使用 Discord **“关注公告频道(Follow)”**<br>方式 B：邀请免费 K-Pop 通知 Bot（如 Weverse Bot） | 极简 | 100% 永久免费 | 官方/社区秒级推送，免维护 |
| **SOOP** (Afreeca) | 方式 A：主播个人 Discord 频道的开播广播<br>方式 B：轻量 Webhook 轮询中继 | 简单 | 100% 免费 | 韩国主播多配有个人 Discord 开播推送 |
| **Instagram** | 方式 A：推特/IG 粉丝站开播提醒 + Discord 推特 Bot (Pingcord/Tweetcord)<br>方式 B：IFTTT / Pipedream 免费 Webhook 联动 | 中等 | 免费配额充足 | 解决 IG 官方限制及本地 IP 封禁痛点 |
| **微博** | **RSSHub** (`/weibo/user/:uid`) + **MonitoRSS Discord Bot** | 极简 | 100% 永久免费 | 纯原生免费，自动将微博开播/发博推至频道 |
| **Berriz** | 方式 A：官方/粉丝群 Discord 频道关注(Follow)<br>方式 B：免费 Cloudflare Workers 探针触发 Webhook | 简单 | 100% 免费 | 极轻量，云端无状态探针推送到 Discord |

---

### 3.2 各平台具体配置指南

#### (1) Weverse (`weverse.io`)
* **最佳实践（Discord 频道关注 Follow）**：
  1. 绝大多数 Weverse 艺人（如 BTS, SEVENTEEN, NewJeans, LE SSERAFIM, ENHYPEN 等）均有数十万人的 Reddit / 粉丝官方 Discord。
  2. 这类服务器中都有类似 `#weverse-live` 或 `#media-updates` 的公告频道（带喇叭 📢 图标）。
  3. **操作**：加入该服务器 -> 点击该公告频道顶部的 **Follow (关注)** -> 选择将消息自动转发到你自己的私有 Discord 频道中。
  4. **特点**：无需申请机器人，零成本，官方开播 1~3 秒内即被转发。

#### (2) SOOP (`sooplive.co.kr`)
* **最佳实践**：
  1. 许多 SOOP 知名 BJ / 主播会在自己的 Discord 粉丝群开播时发公告（如 `@everyone 放送开始`）。同理可使用“关注(Follow)”同步到你的服务器。
  2. 若监控无 Discord 的主播，可使用轻量免费工具定期查询公开 API：
     `https://live.sooplive.co.kr/afreeca/player_live_api.php?bjid={主播ID}`
     检测到 `is_live == true` 时，通过 Discord Webhook 向你的频道发送 `https://play.sooplive.co.kr/{主播ID}`。

#### (3) Instagram Live (`instagram.com`)
* **痛点**：本地如果每 120 秒请求一次 Instagram，极易导致 IP 被限流甚至账号被锁。
* **最佳实践**：
  1. **粉丝转推/开播 Alert 镜像**：大型爱豆/网红开播时，推特上有大量的自动 Alert 账号。可在你自己的 Discord 服务器中拉入 **Pingcord** 或 **Tweetcord**（均免费），订阅对应的主播 IG Live Alert。
  2. **轻量自动化（Make.com / IFTTT / Pipedream）**：配置一个免费触发器，监控 Instagram 动态或通过第三方订阅，触发时向 Discord Webhook 发送包含主页链接的消息。

#### (4) 微博直播 (`weibo.com`)
* **最佳实践（RSSHub + MonitoRSS 机器人）**：
  1. 微博博主开播时会同步发送微博动态。
  2. 使用免费开源的 **RSSHub** 生成博主动态源：`https://rsshub.app/weibo/user/{uid}`
  3. 在自己的 Discord 服务器中添加免费著名的 **MonitoRSS Bot**（或任何 RSS to Discord 机器人）。
  4. 输入 `/feed add url:https://rsshub.app/weibo/user/{uid}` 绑定到频道。
  5. 博主一旦开播，Discord 频道立刻收到带微博链接的消息。

#### (5) Berriz (`berriz.in`)
* **平台特性**：Berriz 是基于 community 的直播平台（URL 类似 `https://berriz.in/{community_key}/live/{live_id}`）。
* **最佳实践**：
  1. **社区官方 Discord 镜像**：若该团体有 Discord 频道，直接关注通知频道。
  2. **免服务器云探针（Cloudflare Workers，每日 10 万次免费请求）**：
     只需几十行 JS 部署在 Cloudflare Workers 上，定时查询 `https://svc-api.berriz.in`，发现开播则 `fetch(DISCORD_WEBHOOK_URL)`。本地录制脚本不消耗任何爬虫请求。

---

## 4. 本地配置规则设计（主播与 Discord 消息匹配）

为了让本地程序从 Discord 频道消息中准确识别出“是哪个主播开播了”，我们需要一个轻量、灵活的匹配规则映射表。

推荐在 `config/` 目录下新增规则配置文件 `discord_rules.json`（或直接在 `config.ini` 中维护）：

```json
{
  "channels": [
    "123456789012345678"
  ],
  "anchors": [
    {
      "name": "Weverse_Artist_A",
      "platform": "weverse",
      "record_url": "https://weverse.io/artist_name/live",
      "match_keywords": ["artist_name", "weverse.io/artist_name"],
      "quality": "原画"
    },
    {
      "name": "SOOP_BJ_B",
      "platform": "sooplive",
      "record_url": "https://play.sooplive.co.kr/bj_id",
      "match_keywords": ["bj_id", "sooplive.co.kr/bj_id"],
      "quality": "原画"
    },
    {
      "name": "Instagram_User_C",
      "platform": "instagram",
      "record_url": "https://www.instagram.com/username/live/",
      "match_keywords": ["instagram.com/username", "username is live"],
      "quality": "原画"
    },
    {
      "name": "Weibo_Anchor_D",
      "platform": "weibo",
      "record_url": "https://weibo.com/u/1234567890",
      "match_keywords": ["1234567890", "微博直播"],
      "quality": "原画"
    },
    {
      "name": "Berriz_Artist_E",
      "platform": "berriz",
      "record_url": "https://berriz.in/community_name",
      "match_keywords": ["community_name", "berriz.in/community_name"],
      "quality": "原画"
    }
  ]
}
```

### 匹配算法规则：
当 Discord 监听到任何一条消息（包括消息正文 `content`、卡片 `embed.title`、`embed.description`、`embed.url`）：
1. 遍历注册的规则列表。
2. 若 `match_keywords` 中任意一个关键词出现在消息中，或者消息中的 URL 与 `record_url` 关键标识一致，即认定该主播开播。
3. 立即触发对应的 `threading.Event().set()`。

---

## 5. 实施路线图

1. **第一阶段（当前）**：
   - 确立 5 个目标平台的 Discord 开播通知输入途径（Discord Server / Webhook / Follow）。
   - 建立本地配置格式规范（主播标识、关键词映射、频道 ID、备用超时时间）。
2. **第二阶段**：
   - 实现轻量后台 `DiscordMonitor` 模块（基于 `discord.py` 或轻量 WebSocket）。
   - 在独立脚本中模拟接收 5 个平台的 Discord 消息并测试关键词匹配率。
3. **第三阶段**：
   - 无缝集成进 `main.py` 的 `start_record()` 循环，实现事件驱动与备用慢速轮询状态机切换。
4. **第四阶段**：
   - 验证异常场景：模拟 Discord 掉线自动降级为高频轮询、模拟假消息防误报等容灾测试。
