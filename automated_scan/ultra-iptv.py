#!/usr/bin/env python3
import requests
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed

INPUT_FILE = "iptv.txt"
OUTPUT_FILE = "collect/playlist.m3u"

REMOVE_DUPLICATES = False
MAX_THREADS = 200
TIMEOUT = 60

session = requests.Session()
session.headers.update({"User-Agent": "IPTV-Merger/3.0"})

def fetch(url):
    try:
        print(f"[→] Fetching: {url}")
        r = session.get(url, timeout=TIMEOUT, allow_redirects=True)

        code = r.status_code

        if code >= 400:
            print(f"[✗] HTTP {code} ERROR: {url}")
            return url, None

        if not r.text.strip():
            print(f"[!] Empty playlist: {url}")
            return url, None

        print(f"[✓] OK {code}: {url} ({len(r.text)} bytes)")
        r.encoding = r.encoding or r.apparent_encoding
        return url, r.text

    except requests.exceptions.Timeout:
        print(f"[⏱] Timeout: {url}")
    except requests.exceptions.ConnectionError:
        print(f"[⚠] Connection failed: {url}")
    except requests.exceptions.RequestException as e:
        print(f"[ERR] {url} -> {e}")

    return url, None


def parse_m3u(base, text):
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    out = []
    i = 0

    while i < len(lines):
        if lines[i].lower().startswith("#extinf"):
            info = lines[i]
            i += 1
            while i < len(lines) and lines[i].startswith("#"):
                info += "\n" + lines[i]
                i += 1
            if i < len(lines):
                out.append((info, urljoin(base, lines[i])))

        elif lines[i].startswith("http"):
            out.append((None, lines[i]))

        i += 1

    print(f"[i] Parsed {len(out)} entries from {base}")
    return out


def load_urls():
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        return [u.strip() for u in f if u.strip() and not u.startswith("#")]


def merge_all(start_urls):
    visited = set()
    collected = []
    queue = list(start_urls)

    while queue:
        batch = [u for u in queue if u not in visited]
        queue = []

        print(f"\n[+] Fetching {len(batch)} playlists in parallel…")

        with ThreadPoolExecutor(MAX_THREADS) as executor:
            futures = [executor.submit(fetch, u) for u in batch]

            for future in as_completed(futures):
                url, text = future.result()
                visited.add(url)

                if not text:
                    continue

                items = parse_m3u(url, text)

                for extinf, link in items:
                    if "type=m3u" in link.lower() or link.lower().endswith(".m3u"):
                        if link not in visited:
                            print(f"[↻] Found nested playlist → {link}")
                            queue.append(link)
                    else:
                        collected.append((extinf, link))

    print(f"\n[OK] Total streams collected: {len(collected)}")
    return collected


def dedupe(entries):
    seen = set()
    unique = []
    for extinf, url in entries:
        if url in seen:
            continue
        seen.add(url)
        unique.append((extinf, url))
    return unique


def write(entries):
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write("#EXTM3U\n")
        for extinf, url in entries:
            if extinf:
                f.write(extinf + "\n")
            else:
                f.write(f"#EXTINF:-1,{url}\n")
            f.write(url + "\n")

    print(f"[✔] Final playlist saved → {OUTPUT_FILE}")


urls = load_urls()
print(f"[i] Loaded {len(urls)} source URLs")

entries = merge_all(urls)

if REMOVE_DUPLICATES:
    print("[i] Removing duplicate stream URLs…")
    entries = dedupe(entries)

write(entries)
print(f"[DONE] Playlist contains {len(entries)} unique channels")

