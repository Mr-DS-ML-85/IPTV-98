#!/usr/bin/env python3
"""
m3u-dedupe.py

Remove duplicate URLs from .m3u/.m3u8 playlists while keeping the first occurrence's metadata.

Usage:
    python3 m3u-dedupe.py playlist.m3u
    python3 m3u-dedupe.py /path/to/dir   # will process all .m3u/.m3u8 files in that dir
"""

from pathlib import Path
import sys

def normalize_url(u: str) -> str:
    """Basic normalization used for duplicate detection.
    - strip whitespace
    - remove trailing slash (optional)
    Keep this conservative: do not remove query parameters by default.
    """
    if not u:
        return u
    nu = u.strip()
    # remove a single trailing slash to avoid trivial duplicates
    if nu.endswith('/') and not nu.startswith('rtmp://'):
        nu = nu[:-1]
    return nu

def process_file(path: Path):
    text = path.read_text(encoding='utf-8', errors='ignore').splitlines()
    if not text:
        print(f"[!] {path} is empty, skipping.")
        return

    # keep header (#EXTM3U) if present
    out_lines = []
    removed_urls = []
    seen = set()

    i = 0
    # preserve header (if first non-empty line is #EXTM3U)
    # find first non-empty line index
    first_nonempty = None
    for idx, line in enumerate(text):
        if line.strip() != "":
            first_nonempty = idx
            break
    if first_nonempty is not None and text[first_nonempty].strip().upper() == "#EXTM3U":
        out_lines.append("#EXTM3U")
        i = first_nonempty + 1
    else:
        i = 0

    # We'll accumulate a metadata buffer (lines starting with '#') until a non-# non-empty line (URL)
    metadata_buf = []
    total_urls = 0
    kept_urls = 0
    dup_count = 0

    while i < len(text):
        line = text[i]
        stripped = line.strip()
        if stripped == "":
            # blank line: flush as-is (optional: keep)
            # We won't write blank lines to output (keeps file tidy)
            metadata_buf = []
            i += 1
            continue

        if stripped.startswith("#"):
            # metadata/comment line
            metadata_buf.append(line)
            i += 1
            continue

        # a non-comment line — treat as URL (or path)
        url_line = stripped
        total_urls += 1
        norm = normalize_url(url_line)

        if norm in seen:
            # duplicate — skip, but count
            dup_count += 1
            removed_urls.append(url_line)
            # clear metadata buffer because those metadata lines correspond to the skipped URL
            metadata_buf = []
        else:
            # new URL -> keep metadata + url
            seen.add(norm)
            kept_urls += 1
            if metadata_buf:
                out_lines.extend(metadata_buf)
            else:
                # ensure there's at least a generic EXTINF if none provided:
                # But we will NOT fabricate metadata; we simply write the URL alone if no metadata exists
                pass
            out_lines.append(url_line)
            metadata_buf = []

        i += 1

    # write output only if we have something to write
    out_path = path.with_name(f"{path.stem}_dedup{path.suffix}")
    with open(out_path, "w", encoding="utf-8") as f:
        for ln in out_lines:
            f.write(ln.rstrip() + "\n")

    dup_path = path.with_name(f"{path.stem}_duplicates.txt")
    with open(dup_path, "w", encoding="utf-8") as f:
        for u in removed_urls:
            f.write(u + "\n")

    print(f"[OK] {path.name}: total URLs={total_urls}, kept={kept_urls}, duplicates_removed={dup_count}")
    print(f"     deduped -> {out_path.name}, duplicates list -> {dup_path.name}")

def process_path(p: str):
    pth = Path(p)
    if not pth.exists():
        print(f"[ERR] Path not found: {p}")
        return

    if pth.is_file():
        # single file
        process_file(pth)
    elif pth.is_dir():
        # process all .m3u / .m3u8 files
        files = sorted([f for f in pth.iterdir() if f.is_file() and f.suffix.lower() in ('.m3u', '.m3u8')])
        if not files:
            print(f"[!] No .m3u/.m3u8 files found in directory: {p}")
            return
        for f in files:
            process_file(f)
    else:
        print(f"[ERR] Unsupported path type: {p}")

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 m3u-dedupe.py <playlist.m3u> | <dir>")
        sys.exit(1)

    for arg in sys.argv[1:]:
        process_path(arg)

if __name__ == "__main__":
    main()
