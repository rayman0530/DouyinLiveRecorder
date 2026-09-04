# -*- coding: utf-8 -*-
import json
import requests
import uuid
import threading
import time
import logging

logger = logging.getLogger(__name__)

_weverse_refresh_lock = threading.Lock()
_cached_tokens = {
    "access": None,
    "refresh": None,
    "last_refresh_time": 0
}


def refresh_weverse_token(refresh_token, proxy_addr=None, current_access_token=None):
    if not refresh_token:
        return None, None

    global _cached_tokens
    with _weverse_refresh_lock:
        now = time.time()
        # If refreshed within the last 60 seconds and we have fresh tokens in memory:
        if _cached_tokens["access"] and (now - _cached_tokens["last_refresh_time"] < 60):
            if not current_access_token or current_access_token != _cached_tokens["access"]:
                return _cached_tokens["access"], _cached_tokens["refresh"]

        refresh_url = "https://accountapi.weverse.io/api/v1/token/refresh"
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Origin": "https://www.weverse.io",
            "Referer": "https://www.weverse.io/",
            "X-ACC-SERVICE-ID": "weverse",
            "X-ACC-APP-SECRET": "5419526f1c624b38b10787e5c10b2a7a",
            "X-ACC-TRACE-ID": str(uuid.uuid4())
        }

        # If cache has a newer refresh token, prefer sending that
        token_to_send = _cached_tokens["refresh"] if (_cached_tokens["refresh"] and _cached_tokens["last_refresh_time"] > 0) else refresh_token

        payload = {
            "refreshToken": token_to_send
        }

        proxies = None
        if proxy_addr:
            if not proxy_addr.startswith(('http://', 'https://', 'socks5://')):
                proxy_addr = f'http://{proxy_addr}'
            proxies = {
                "http": proxy_addr,
                "https": proxy_addr
            }

        try:
            response = requests.post(refresh_url, json=payload, headers=headers, proxies=proxies, timeout=15)
            if response.status_code == 200:
                data = response.json()
                new_access_token = data.get("accessToken")
                new_refresh_token = data.get("refreshToken")
                _cached_tokens["access"] = new_access_token
                _cached_tokens["refresh"] = new_refresh_token
                _cached_tokens["last_refresh_time"] = time.time()
                return new_access_token, new_refresh_token
            else:
                logger.error(f"[Weverse Auth] Token refresh failed, status: {response.status_code}, body: {response.text}")
                return None, None
        except Exception as e:
            logger.error(f"[Weverse Auth] Token refresh request error: {e}")
            return None, None
