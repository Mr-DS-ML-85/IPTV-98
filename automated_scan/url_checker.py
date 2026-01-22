#!/usr/bin/env python3
import sys
import os
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin

# ------------- CONFIG -------------
INPUT_FILE = "collect/playlist_dedup.m3u"
OUTPUT_FILE = "final/play.m3u"
TIMEOUT = 10.0          # Increased for stability
MAX_THREADS = 300       # Lowered slightly to avoid IP bans from servers
# ----------------------------------

class M3UValidator:
    def __init__(self):
        self.session = requests.Session()
        # Pretend to be VLC to avoid being blocked
        self.session.headers.update({
            "User-Agent": "VLC/3.0.18 LibVLC/3.0.18",
            "Accept": "*/*"
        })

    def validate_url(self, item):
        url = item['url']
        try:
            # We use stream=True so we only download the headers, not the video bytes
            response = self.session.get(url, timeout=TIMEOUT, stream=True, allow_redirects=True)
            
            if response.status_code == 200:
                ct = response.headers.get('Content-Type', '').lower()
                # Accept anything that looks like video or a playlist
                valid_types = ['video', 'mpegurl', 'application/octet-stream', 'apple.mpegurl', 'binary/octet-stream']
                
                if any(t in ct for t in valid_types) or url.split('?')[0].endswith(('.ts', '.m3u8', '.mp4')):
                    print(f"  [✓] VALID: {item['name']}")
                    return item
            
            print(f"  [✗] BAD STICKY/404: {item['name']} ({response.status_code})")
        except Exception as e:
            print(f"  [⏱] TIMEOUT/FAIL: {item['name']}")
        
        return None

    def parse_m3u(self, path):
        if not os.path.exists(path):
            print(f"File {path} not found.")
            return []
        
        items = []
        with open(path, 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()
            
        current_metadata = ""
        for line in lines:
            line = line.strip()
            if line.startswith("#EXTINF"):
                current_metadata = line
            elif line.startswith("http"):
                # Extract a friendly name from metadata
                name = current_metadata.split(",")[-1] if "," in current_metadata else "Unknown"
                items.append({'metadata': current_metadata, 'url': line, 'name': name})
        return items

    def run(self):
        print(f"Reading {INPUT_FILE}...")
        raw_items = self.parse_m3u(INPUT_FILE)
        if not raw_items:
            return

        print(f"Validating {len(raw_items)} streams using {MAX_THREADS} threads...")
        valid_list = []

        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            futures = [executor.submit(self.validate_url, item) for item in raw_items]
            for future in as_completed(futures):
                result = future.result()
                if result:
                    valid_list.append(result)

        print(f"\nFound {len(valid_list)} working streams. Writing to {OUTPUT_FILE}...")
        
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            f.write("#EXTM3U\n")
            for item in valid_list:
                f.write(f"{item['metadata']}\n{item['url']}\n")

if __name__ == "__main__":
    validator = M3UValidator()
    validator.run()
