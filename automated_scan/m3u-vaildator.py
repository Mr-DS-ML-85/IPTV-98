#!/usr/bin/env python3 -u
"""
m3u-cleaner.py

- No argparse: accepts optional positional file args (local paths or URLs).
- Uses requests with a Retry(total=1).
- Concurrent validation uses ThreadPoolExecutor with MAX_THREADS = 120.
- Writes cleaned playlist to OUTPUT_FILE if provided / default.
"""

import sys
import os
from urllib.parse import urlparse, urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import m3u8
from termcolor import colored

# ------------- CONFIG -------------
# You may edit these defaults at the top of the file.
DEFAULT_INPUTS = ["collect/playlist_dedup.m3u"]   # tried in order if no args provided
OUTPUT_FILE = "final/playtv.m3u"         # final output file written if there are valid streams
BLACKLIST_FILE = ""                          # set to "blacklist.txt" to use a blacklist
DEBUG = False                                # set True to enable debug prints
TIMEOUT = 1.0                                # seconds (same as original default)
MAX_THREADS = 300                            # CONCURRENT WORKERS (you asked for 120)
RETRY_TOTAL = 1                              # only 1 retry (total attempts = 1 + original attempt is controlled by urllib3)
CHECK_FIRST_N_SEGMENTS = 5
MAX_NESTED_PLAYLIST_DEPTH = 6
# -----------------------------------

# configure requests session with Retry(total=RETRY_TOTAL)
session = requests.Session()
session.headers.update({"User-Agent": "m3u-cleaner/1.0"})
retry_strategy = Retry(
    total=RETRY_TOTAL,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=frozenset(["HEAD", "GET", "OPTIONS"]),
    backoff_factor=0.5
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("http://", adapter)
session.mount("https://", adapter)

def nice_print(message, colour=None, indent=0, debug=False):
    """
    Just prints things in colour with consistent indentation
    """
    if debug and not DEBUG:
        return

    if colour is None:
        if 'OK' in message or '✓' in message:
            colour = 'green'
        elif 'ERROR' in message or '✗' in message or '!' in message:
            colour = 'red'

    print(colored('{0}{1}'.format(' ' * indent * 2, message), colour))

# ----------------- Fetching / parsing helpers -----------------

def is_url(s):
    return s.startswith("http://") or s.startswith("https://")

def read_local_file(path):
    with open(path, "r", encoding="utf-8") as f:
        content = f.readlines()
    return [x.strip() for x in content]

def fetch_text(url):
    try:
        resp = session.get(url, timeout=TIMEOUT, allow_redirects=True)
        if resp.status_code >= 400:
            nice_print(f"[✗] HTTP {resp.status_code} ERROR: {url}")
            return None
        if not resp.text or not resp.text.strip():
            nice_print(f"[!] Empty playlist: {url}")
            return None
        resp.encoding = resp.encoding or resp.apparent_encoding
        nice_print(f"[✓] OK {resp.status_code}: {url} ({len(resp.text)} bytes)")
        return resp.text
    except requests.exceptions.Timeout:
        nice_print(f"[⏱] Timeout: {url}", debug=True)
    except requests.exceptions.ConnectionError:
        nice_print(f"[⚠] Connection failed: {url}", debug=True)
    except requests.exceptions.RequestException as e:
        nice_print(f"[ERR] {url} -> {e}", debug=True)
    return None

def parse_m3u(base, text):
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    out = []
    i = 0
    while i < len(lines):
        if lines[i].lower().startswith("#extinf"):
            info = lines[i]
            i += 1
            # allow multiple metadata lines after EXTINF
            while i < len(lines) and lines[i].startswith("#"):
                info += "\n" + lines[i]
                i += 1
            if i < len(lines):
                out.append((info, urljoin(base, lines[i])))
        elif lines[i].startswith("http"):
            out.append((None, lines[i]))
        i += 1
    nice_print(f"[i] Parsed {len(out)} entries from {base}")
    return out

# ----------------- Validation (from original script) -----------------

def verify_video_link(url, timeout=TIMEOUT, indent=1):
    """
    Verifies a video stream link works (HEAD + content-type heuristic).
    """
    nice_print('Loading video: {0}'.format(url), indent=indent, debug=True)

    indent = indent + 1
    parsed_url = urlparse(url)

    # original enforced .ts; keep behavior but be slightly tolerant
    if not parsed_url.path.endswith('.ts'):
        # don't immediately reject — use HEAD checks
        pass

    try:
        r = session.head(url, timeout=(timeout, timeout), allow_redirects=True)
    except Exception as e:
        nice_print('ERROR loading video URL: {0}'.format(str(e)[:100]), indent=indent, debug=True)
        return False
    else:
        headers = r.headers or {}
        ct = headers.get('Content-Type', '').lower()
        video_stream = ('video' in ct) or ('octet-stream' in ct) or ('mpegurl' in ct) or ('application/vnd.apple.mpegurl' in ct)
        if r.status_code != 200:
            nice_print(f'ERROR {r.status_code} video URL', indent=indent, debug=True)
            return False
        elif video_stream:
            nice_print('OK loading video data', indent=indent, debug=True)
            return True
        else:
            # fallback accept .ts by path if content-type not informative
            if parsed_url.path.endswith('.ts'):
                nice_print('OK (fallback) .ts video URL (content-type not informative)', indent=indent, debug=True)
                return True
            nice_print('ERROR unknown URL: {0}'.format(url), indent=indent, debug=True)
            return False

def verify_playlist_link(url, timeout=TIMEOUT, indent=1, check_first_N_only=CHECK_FIRST_N_SEGMENTS):
    nice_print('Loading playlist: {0}'.format(url), indent=indent, debug=True)

    if indent > MAX_NESTED_PLAYLIST_DEPTH:
        nice_print('ERROR nested playlist too deep', indent=indent)
        return False

    # check for redirect to non-m3u file
    try:
        m3u8_head = session.head(url, timeout=(timeout, timeout), allow_redirects=False)
        if 300 <= m3u8_head.status_code < 400:
            m3u8_head2 = session.head(url, timeout=(timeout, timeout), allow_redirects=True)
            # try to find redirected location safely
            redirected_url = m3u8_head2.history[-1].headers.get('Location') if m3u8_head2.history else m3u8_head2.url
            extension = urlparse(redirected_url).path.split(".")[-1] if redirected_url else ''
            if extension not in ("m3u8", "m3u"):
                nice_print('ERROR m3u8-playlist 30x-redirected to "{0}"-filetype. Skipping this.'.format(extension), indent=indent, debug=False)
                return False
    except Exception as e:
        nice_print('ERROR loading redirected playlist: {0}'.format(str(e)[:100]), indent=indent, debug=True)
        return False

    try:
        m3u8_obj = m3u8.load(url, timeout=timeout)
    except Exception as e:
        nice_print('ERROR loading playlist: {0}'.format(str(e)[:100]), indent=indent, debug=True)
        return False

    if 0 == len(m3u8_obj.data.get('playlists', [])) + len(m3u8_obj.data.get('segments', [])):
        nice_print('ERROR: playlist is empty.', indent=indent, debug=True)
        return False

    # if nested playlists exist, verify first nested
    for nested_playlist in m3u8_obj.data.get('playlists', []):
        nested_uri = nested_playlist.get('uri')
        if nested_uri:
            if nested_uri.startswith(('https://', 'http://')):
                nested_url = nested_uri
            else:
                nested_url = f'{m3u8_obj.base_uri}{nested_uri}'
            return verify_playlist_link(nested_url, timeout=timeout, indent=indent+1, check_first_N_only=check_first_N_only)

    counter = 0
    for segment in m3u8_obj.data.get('segments', []):
        seg_uri = segment.get('uri')
        if not seg_uri:
            continue
        if seg_uri.startswith(('https://', 'http://')):
            seg_url = seg_uri
        else:
            seg_url = f'{m3u8_obj.base_uri}{seg_uri}'

        if not verify_video_link(seg_url, timeout=timeout, indent=indent+1):
            return False  # first bad segment invalidates playlist
        counter += 1
        if counter >= check_first_N_only:
            remaining = max(0, len(m3u8_obj.data.get('segments', [])) - counter)
            nice_print('OK: skipping tests of remaining {0} entries because we have {1} good files already in this playlist'.format(remaining, counter), indent=indent, debug=True)
            return True

    return True

def verify_playlist_item(item, timeout=TIMEOUT):
    nice_title = (item.get('metadata') or '').split(',')[-1]
    nice_print('{0} | {1}'.format(nice_title, item.get('url')), colour='yellow')

    indent = 1
    url = item.get('url')
    if not url:
        return False

    lower = url.lower()
    # direct ts
    if lower.endswith('.ts'):
        ok = verify_video_link(url, timeout, indent)
        nice_print('OK video data' if ok else 'ERROR video data', indent=indent)
        return ok
    # playlist
    if lower.endswith('.m3u8') or 'type=m3u' in lower or 'x-mpegurl' in lower:
        ok = verify_playlist_link(url, timeout, indent)
        nice_print('OK playlist data' if ok else 'ERROR playlist data', indent=indent)
        return ok
    # generic HEAD
    try:
        r = session.head(url, timeout=(timeout, timeout), allow_redirects=True)
    except Exception as e:
        nice_print(f'ERROR loading URL: {str(e)[:100]}', indent=indent, debug=True)
        return False
    headers = r.headers or {}
    ct = headers.get('Content-Type', '').lower()
    video_stream = 'video' in ct or 'octet-stream' in ct
    playlist_link = 'x-mpegurl' in ct or 'application/vnd.apple.mpegurl' in ct

    if r.status_code != 200:
        nice_print(f'ERROR {r.status_code} loading URL: {url}', indent=indent, debug=True)
        return False
    if video_stream:
        nice_print('OK loading video data', indent=indent, debug=True)
        return True
    if playlist_link:
        return verify_playlist_link(url, timeout, indent + 1)
    # fallback
    parsed_url = urlparse(url)
    if parsed_url.path.endswith('.ts'):
        nice_print('OK (fallback) .ts video URL (no informative content-type)', indent=indent, debug=True)
        return True
    nice_print(f'ERROR unknown URL: {url}', indent=indent, debug=True)
    return False

# ----------------- Filtering and concurrent validation -----------------

def load_blacklist(path):
    if not path:
        return set()
    try:
        with open(path, "r", encoding="utf-8") as f:
            items = [x.strip() for x in f if x.strip()]
            return set(items)
    except Exception as e:
        print(f"blacklist file issue: {type(e)} {e}")
        return set()

def build_items_from_m3u_files(m3u_files, blacklist_set):
    playlist_items = []
    num_blacklisted = 0
    for m3u_file in m3u_files:
        try:
            content = read_local_file(m3u_file)
        except IsADirectoryError:
            continue
        if not content:
            continue
        if content[0] != '#EXTM3U' and content[0].encode("ascii", "ignore").decode("utf-8").strip() != '#EXTM3U':
            raise Exception('Invalid file, no EXTM3U header in "{0}"'.format(m3u_file))

        url_indexes = [i for i, s in enumerate(content) if s.startswith('http')]
        if len(url_indexes) < 1:
            raise Exception('Invalid file, no URLs in "{0}"'.format(m3u_file))

        for u in url_indexes:
            if content[u] in blacklist_set:
                num_blacklisted += 1
            else:
                detail = {
                    'metadata': content[u - 1],
                    'url': content[u]
                }
                playlist_items.append(detail)

    if num_blacklisted:
        print(f'Input list reduced by {num_blacklisted} items, because those urls are on the blacklist.')

    return playlist_items

def filter_streams_concurrent(m3u_files, timeout, blacklist_file):
    """
    Load M3U files, build playlist_items and validate them concurrently using MAX_THREADS.
    """
    blacklist_set = load_blacklist(blacklist_file)
    playlist_items = build_items_from_m3u_files(m3u_files, blacklist_set)

    print(f'Input list now has {len(playlist_items)} entries, patience please. Timeout for each test is {timeout} seconds.')

    filtered = []
    print(f"\n[+] Validating {len(playlist_items)} streams in parallel (max {MAX_THREADS})…")

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        future_to_item = {executor.submit(verify_playlist_item, item, timeout): item for item in playlist_items}
        for future in as_completed(future_to_item):
            item = future_to_item[future]
            try:
                ok = future.result()
            except Exception as e:
                nice_print(f"ERROR validating {item.get('url')} -> {e}", colour='red')
                ok = False
            if ok:
                filtered.append(item)

    print(f'\n{len(playlist_items) - len(filtered)} items filtered out of {len(playlist_items)} in total')
    return filtered

# ----------------- Output write -----------------

def write_output_file(entries, path=OUTPUT_FILE):
    with open(path, "w", encoding="utf-8") as output_file:
        output_file.write('#EXTM3U\n')
        output_file.writelines(['{0}\n{1}\n'.format(item['metadata'], item['url']) for item in entries])
    print(f'Writing to {path}')

# ----------------- Main -----------------

def find_default_inputs():
    # if user provided sys.argv inputs (positional), use them
    if len(sys.argv) > 1:
        # skip the script name
        return [arg for arg in sys.argv[1:] if arg.strip()]
    # otherwise try DEFAULT_INPUTS existing files
    for fn in DEFAULT_INPUTS:
        if os.path.exists(fn):
            return [fn]
    # fallback to any .m3u files in cwd
    m3us = [f for f in os.listdir('.') if f.endswith('.m3u') or f.endswith('.m3u8')]
    if m3us:
        return m3us
    print("No input files found. Please provide playlist file(s) as positional arguments or create 'iptv.txt'/'input.m3u'.")
    sys.exit(1)

if __name__ == '__main__':
    m3u_files = find_default_inputs()
    filtered_items = filter_streams_concurrent(m3u_files, TIMEOUT, BLACKLIST_FILE)

    if filtered_items and OUTPUT_FILE:
        write_output_file(filtered_items, OUTPUT_FILE)
    else:
        print("No valid streams found or no output file configured.")
