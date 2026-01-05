# -*- coding: utf-8 -*-
import os
import time
import requests
import re
from urllib.parse import urljoin
import threading

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
        self.stop_flag = False
        self.error_count = 0
        self.max_errors = 10
        self.failed = False

    def stop(self):
        self.stop_flag = True

    def start(self):
        print(f"Starting Native HLS Download: {self.output_path}")
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

        with open(self.output_path, 'wb') as f:
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
                            break
                        time.sleep(5)
                        continue
                    
                    self.error_count = 0 # Reset on success
                    content = resp.text
                    
                    # Check for end of stream
                    if '#EXT-X-ENDLIST' in content:
                        print("Stream ended (EXT-X-ENDLIST).")
                        break

                    # Parse Header Info
                    seq_match = re.search(r'#EXT-X-MEDIA-SEQUENCE:(\d+)', content)
                    current_seq = int(seq_match.group(1)) if seq_match else 0
                    
                    target_duration_match = re.search(r'#EXT-X-TARGETDURATION:(\d+)', content)
                    target_duration = float(target_duration_match.group(1)) if target_duration_match else 5.0
                    
                    # Parse Segments
                    lines = content.splitlines()
                    segments = []
                    for i, line in enumerate(lines):
                        if line.startswith('#EXTINF:'):
                            # Extract duration if needed, e.g. #EXTINF:1.001,
                            # dur = float(line.split(':')[1].split(',')[0])
                            
                            url_line = lines[i+1] if i+1 < len(lines) else None
                            if url_line and not url_line.startswith('#'):
                                segments.append(url_line)
                    
                    # Logic to find NEW segments
                    local_seq = current_seq
                    new_segments_found = False
                    
                    for seg_url in segments:
                        if local_seq > self.last_seq:
                            # We found a new segment
                            full_url = urljoin(self.m3u8_url, seg_url.strip())
                            
                            # Download Segment
                            if self.download_segment(full_url, f):
                                self.last_seq = local_seq
                                new_segments_found = True
                            else:
                                print(f"Failed to download segment {local_seq}")
                                # If we fail a segment, we might want to break or continue?
                                # Continuing risks corruption, but better than stopping.
                        
                        local_seq += 1
                    
                    if not new_segments_found:
                        # If no new segments, wait roughly half target duration or until refreshed
                        # But don't hammer server.
                        # HLS spec says reload time should be target duration.
                        # But we want low latency? No, we want recording.
                        # Typically wait target_duration / 2 is safe.
                        sleep_time = max(1.0, target_duration / 2)
                        time.sleep(sleep_time)
                    else:
                        # We processed segments. Wait target duration before checking again?
                        # Or check sooner?
                        # If we just caught up, we should wait.
                        time.sleep(target_duration)

                except Exception as e:
                    print(f"Download Loop Error: {e}")
                    self.error_count += 1
                    time.sleep(5)
                    if self.error_count > self.max_errors:
                         self.failed = True
                         break

    def download_segment(self, url, file_handle):
        for _ in range(3): # Retry 3 times
            if self.stop_flag:
                return False
            try:
                # Use stream=True to avoid loading big segments into memory (though they are small usually)
                s_resp = self.session.get(url, timeout=20, stream=True)
                if s_resp.status_code == 200:
                    for chunk in s_resp.iter_content(chunk_size=65536):
                        if self.stop_flag:
                            return False
                        file_handle.write(chunk)
                    
                    file_handle.flush() # Ensure data is written to disk
                    try:
                        os.fsync(file_handle.fileno()) # Force write to disk
                    except:
                        pass
                    return True
                else:
                    # print(f"Segment status {s_resp.status_code} for {url}")
                    pass
            except Exception as e:
                # print(f"Segment fetch error: {e}")
                pass
            time.sleep(1)
        return False
