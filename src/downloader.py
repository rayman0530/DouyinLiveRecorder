# -*- coding: utf-8 -*-
import os
import time
import requests
import re
from urllib.parse import urljoin
import threading
import queue

class NativeHLSDownloader:
    def __init__(self, m3u8_url: str, output_path: str, headers: dict = None):
        self.m3u8_url = m3u8_url
        self.output_path = output_path
        self.headers = headers or {}
        # Ensure we look like a browser/player
        if 'User-Agent' not in self.headers:
             self.headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
        self.last_seq = -1
        self.last_init_url = None
        self.stop_flag = False
        self.error_count = 0
        self.max_errors = 10
        self.failed = False
        
        self.segment_queue = queue.Queue()
        self.download_thread = None

    def stop(self):
        self.stop_flag = True

    def start(self):
        print(f"Starting Native HLS Download: {self.output_path}")
        # Debug: Show headers (mask cookie for safety)
        safe_headers = {k: (v[:20] + "..." if k.lower() == 'cookie' else v) for k, v in self.headers.items()}
        print(f"Downloader Headers: {safe_headers}")
        os.makedirs(os.path.dirname(os.path.abspath(self.output_path)), exist_ok=True)
        
        # Resolve Master Playlist if needed
        try:
            resp = self.session.get(self.m3u8_url, timeout=15)
            if resp.status_code == 200:
                content = resp.text
                if '#EXT-X-STREAM-INF' in content:
                    print("Master Playlist detected, selecting best variant...")
                    lines = content.splitlines()
                    variants = []
                    for i, line in enumerate(lines):
                        if line.startswith('#EXT-X-STREAM-INF:'):
                            bw_match = re.search(r'BANDWIDTH=(\d+)', line)
                            bandwidth = int(bw_match.group(1)) if bw_match else 0
                            
                            url_line = lines[i+1] if i+1 < len(lines) else None
                            if url_line and not url_line.startswith('#'):
                                full_url = urljoin(self.m3u8_url, url_line.strip())
                                variants.append({'bandwidth': bandwidth, 'url': full_url})
                    
                    if variants:
                        variants.sort(key=lambda x: x['bandwidth'], reverse=True)
                        best_variant = variants[0]
                        print(f"Selected variant with bandwidth {best_variant['bandwidth']}")
                        self.m3u8_url = best_variant['url']
        except Exception as e:
            print(f"Error checking playlist type: {e}")

        # Start Download Worker
        self.download_thread = threading.Thread(target=self.download_worker)
        self.download_thread.start()

        # Playlist Polling Loop (Producer)
        while not self.stop_flag:
            try:
                start_time = time.time()
                
                try:
                    resp = self.session.get(self.m3u8_url, timeout=15)
                except Exception as e:
                    print(f"Network error fetching playlist: {e}")
                    self.error_count += 1
                    time.sleep(2)
                    continue

                if resp.status_code != 200:
                    print(f"Playlist fetch failed: {resp.status_code}")
                    self.error_count += 1
                    if self.error_count > self.max_errors:
                        print("Too many errors, stopping download.")
                        self.failed = True
                        self.stop_flag = True
                        break
                    time.sleep(5)
                    continue
                
                self.error_count = 0 # Reset on success
                content = resp.text
                
                # Check for end of stream
                if '#EXT-X-ENDLIST' in content:
                    print("Stream ended (EXT-X-ENDLIST).")
                    self.stop_flag = True
                    break

                # Parse Header Info
                seq_match = re.search(r'#EXT-X-MEDIA-SEQUENCE:(\d+)', content)
                current_seq = int(seq_match.group(1)) if seq_match else 0

                # Check for Initialization Segment (fMP4)
                map_match = re.search(r'#EXT-X-MAP:URI="(.*?)"', content)
                if map_match:
                    init_uri = map_match.group(1)
                    full_init_url = urljoin(self.m3u8_url, init_uri)
                    
                    if full_init_url != self.last_init_url:
                        print(f"Queueing Initialization Segment: {full_init_url}")
                        self.segment_queue.put({
                            'type': 'init',
                            'url': full_init_url
                        })
                        self.last_init_url = full_init_url
                
                target_duration_match = re.search(r'#EXT-X-TARGETDURATION:(\d+)', content)
                target_duration = float(target_duration_match.group(1)) if target_duration_match else 5.0
                
                # Parse Segments
                lines = content.splitlines()
                segments = []
                for i, line in enumerate(lines):
                    if line.startswith('#EXTINF:'):
                        byte_range = None
                        # Look ahead for URL and other tags
                        j = i + 1
                        while j < len(lines):
                            next_line = lines[j].strip()
                            if not next_line: # Skip empty lines
                                j += 1
                                continue
                            if next_line.startswith('#'):
                                if next_line.startswith('#EXT-X-BYTERANGE:'):
                                    byte_range = next_line.split(':')[1]
                                # If we hit another EXTINF before finding a URL, stop
                                if next_line.startswith('#EXTINF:'):
                                    break
                                j += 1
                                continue
                            
                            # Found URL
                            segments.append({'url': next_line, 'range': byte_range})
                            break
                
                # Logic to find NEW segments
                local_seq = current_seq
                new_segments_found = False
                
                for seg in segments:
                    if local_seq > self.last_seq:
                        # We found a new segment
                        full_url = urljoin(self.m3u8_url, seg['url'].strip())
                        
                        # Add to Queue
                        self.segment_queue.put({
                            'type': 'segment',
                            'url': full_url,
                            'range': seg['range'],
                            'seq': local_seq
                        })
                        
                        self.last_seq = local_seq
                        new_segments_found = True
                    
                    local_seq += 1
                
                # Polling Sleep Logic
                # Since we decoupled downloading, we can poll aggressively.
                # Target Duration / 2 is good, but for low latency, we might want to ensure we don't drift.
                # If fetch took > target_duration, we are already late, so don't sleep?
                elapsed = time.time() - start_time
                desired_sleep = max(0.5, target_duration / 2)
                
                actual_sleep = desired_sleep
                if elapsed > target_duration:
                     actual_sleep = 0.5 # Minimal sleep if we are slow
                
                time.sleep(actual_sleep)

            except Exception as e:
                print(f"Playlist Polling Error: {e}")
                self.error_count += 1
                time.sleep(5)
                if self.error_count > self.max_errors:
                     self.failed = True
                     self.stop_flag = True
                     break
        
        # Wait for worker to finish queue
        if self.download_thread:
            self.download_thread.join()

    def download_worker(self):
        with open(self.output_path, 'wb') as f:
            while not self.stop_flag or not self.segment_queue.empty():
                try:
                    # Wait for new segment
                    try:
                        item = self.segment_queue.get(timeout=1)
                    except queue.Empty:
                        continue
                    
                    if item['type'] == 'init':
                        self.download_data(item['url'], f)
                    elif item['type'] == 'segment':
                        success = self.download_data(item['url'], f, item['range'])
                        if not success:
                            print(f"Failed to download segment {item.get('seq')}")
                            # We can't easily retry in order if we popped it. 
                            # But download_data has internal retries.
                    
                    self.segment_queue.task_done()
                    
                except Exception as e:
                    print(f"Download Worker Error: {e}")

    def download_data(self, url, file_handle, byte_range=None):
        headers = {}
        if byte_range:
            try:
                if '@' in byte_range:
                    length, offset = byte_range.split('@')
                    start = int(offset)
                    end = start + int(length) - 1
                    headers['Range'] = f'bytes={start}-{end}'
                else:
                     pass
            except Exception as e:
                print(f"Error parsing Byte Range {byte_range}: {e}")

        for _ in range(3): # Retry 3 times
            if self.stop_flag and self.segment_queue.empty(): # Only stop if queue is empty? No, hard stop.
                 # But we want to finish queue if possible? 
                 # If stop_flag is True, start() loop ended. 
                 # Worker loop continues until queue empty.
                 # So here strictly check if we should abort mid-download.
                 pass

            try:
                s_resp = self.session.get(url, timeout=20, stream=True, headers=headers)
                if s_resp.status_code in [200, 206]:
                    for chunk in s_resp.iter_content(chunk_size=65536):
                        # if self.stop_flag: return False # Don't abort mid-write to avoid corruption?
                        file_handle.write(chunk)
                    
                    file_handle.flush()
                    try:
                        os.fsync(file_handle.fileno())
                    except:
                        pass
                    return True
                else:
                    print(f"Segment download failed with status: {s_resp.status_code}")
                    print(f"Response content: {s_resp.text[:200]}") # Print first 200 chars of error
            except Exception as e:
                print(f"Segment download exception: {type(e).__name__}: {e}")
            time.sleep(1)
        return False
