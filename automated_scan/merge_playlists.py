#!/usr/bin/env python3
"""
merge_playlists.py

Merge multiple .m3u / .m3u8 files into one playlist, preserving #EXTINF lines and removing duplicate URLs.

Usage examples:

# 1) Merge all .m3u files inside automated_scan/final into playlist/playtv.m3u
python3 automated_scan/merge_playlists.py automated_scan/final --output playlist/playtv.m3u

# 2) Use shell glob (final/*.m3u) (shell normally expands the glob)
python3 automated_scan/merge_playlists.py automated_scan/final/*.m3u --output playlist/playtv.m3u

# 3) Provide explicit list of files
python3 automated_scan/merge_playlists.py file1.m3u file2.m3u --output playlist/playtv.m3u

Notes:
- If an input is a directory, all *.m3u and *.m3u8 files inside will be included.
- Duplicated URLs are removed; first occurrence wins (including its #EXTINF metadata).
"""
from __future__ import annotations
import sys
import argparse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple, Dict, Optional
import glob

MAX_THREADS = 100

def read_m3u(path: Path) -> List[str]:
    """Read a .m3u/.m3u8 file and return a list of non-empty lines (preserve order)."""
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except Exception as e:
        print(f"[ERROR] Failed to read {path}: {e}")
        return []
    lines = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        # keep everything; header filtering/deduping happens in the merger
        lines.append(line)
    return lines

def expand_inputs(inputs: List[str]) -> List[Path]:
    """Expand globs and directories into a list of existing files (unique, ordered)."""
    expanded: List[Path] = []
    seen = set()
    for inp in inputs:
        # if shell didn't expand a glob (user quoted it), expand here
        if any(ch in inp for ch in "*?[]"):
            for p in sorted(glob.glob(inp)):
                path = Path(p)
                if path.exists() and path.is_file() and path.suffix.lower() in (".m3u", ".m3u8"):
                    if str(path) not in seen:
                        expanded.append(path)
                        seen.add(str(path))
            continue

        p = Path(inp)
        if p.exists():
            if p.is_dir():
                # add files inside directory
                for child in sorted(p.glob("*.m3u")) + sorted(p.glob("*.m3u8")):
                    if str(child) not in seen:
                        expanded.append(child)
                        seen.add(str(child))
            elif p.is_file():
                if p.suffix.lower() in (".m3u", ".m3u8"):
                    if str(p) not in seen:
                        expanded.append(p)
                        seen.add(str(p))
            else:
                # skip non-regular files
                continue
        else:
            print(f"[SKIP] {inp} not found")
    return expanded

def merge_from_file_lines(file_lines: List[str], seen_urls: set, combined: List[str], pending_extinf: Optional[str]) -> Optional[str]:
    """
    Process lines from a single file (list of lines).
    Returns the last extinf pending after processing (or None).
    Appends to combined for new urls. Updates seen_urls.
    """
    ext = pending_extinf
    for line in file_lines:
        # skip playlist header lines
        if line.upper().strip() == "#EXTM3U":
            continue
        if line.startswith("#EXTINF"):
            ext = line
            continue
        if line.startswith("#"):
            # other tags — keep only if they precede a URL? For now skip tags except EXTINF
            continue
        # treat as URL
        url = line.strip()
        if url in seen_urls:
            # duplicate — skip (don't append ext)
            ext = None
            continue
        # new URL — append ext (if present) then url
        if ext:
            combined.append(ext)
        combined.append(url)
        seen_urls.add(url)
        ext = None
    return ext

def main():
    parser = argparse.ArgumentParser(description="Merge multiple .m3u files into one playlist (dedupe by URL).")
    parser.add_argument("inputs", nargs="+", help="Input files, globs (quoted), or directories (e.g. final/ or final/*.m3u)")
    parser.add_argument("--output", "-o", required=True, help="Output file to write (e.g. playlist/playtv.m3u)")
    parser.add_argument("--threads", "-t", type=int, default=MAX_THREADS, help="Thread pool size for parallel file reads")
    args = parser.parse_args()

    output = Path(args.output)
    inputs = args.inputs

    files = expand_inputs(inputs)
    if not files:
        print("[ERROR] No valid .m3u files found in provided inputs.")
        sys.exit(2)

    combined: List[str] = ["#EXTM3U"]
    seen_urls = set()

    # We'll read files concurrently (IO bound) and then process them in the order results complete.
    # To preserve stable merging order you can instead process files in 'files' order sequentially.
    # Here we keep concurrency but still process file contents in the order the futures complete.
    with ThreadPoolExecutor(max_workers=args.threads) as executor:
        futures = {executor.submit(read_m3u, p): p for p in files}

        # We want to preserve each file's internal order; dedupe across all files.
        # Keep track of any pending extinf that wasn't followed by a URL at EOF of a file (rare).
        for future in as_completed(futures):
            p = futures[future]
            try:
                lines = future.result()
            except Exception as e:
                print(f"[ERROR] Reading {p.name} failed: {e}")
                continue
            # merge this file's lines into combined with dedupe
            # we do per-file pending extinf handling in merge_from_file_lines
            pending = merge_from_file_lines(lines, seen_urls, combined, pending_extinf=None)
            # if pending extinf exists at EOF, just ignore it (no following URL)

    # ensure parent dir exists
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(combined) + "\n", encoding="utf-8")

    print(f"\n[DONE] Combined playlist saved as: {output}")
    print(f"[INFO] Total unique entries written (excluding header): {len(combined) - 1}")

if __name__ == "__main__":
    main()
