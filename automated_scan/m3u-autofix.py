#!/usr/bin/env python3
"""
m3u_auto_fix.py — analyze and fix common M3U playlist issues.

Usage:
    python3 m3u_auto_fix.py -i input.m3u -o output.m3u

Options:
    --no-preserve-attrs    Do not preserve original #EXTINF attributes (group-title, tvg-logo) when writing output.
    --no-keep-broken       Do not keep broken EXTINF entries as commented blocks in output (they will be skipped).
    --no-dedupe            Do not remove exact duplicate URLs (keeps duplicates).
    --no-add-default-extinf Do not add default EXTINF for bare URL lines.

Behavior (default):
 - Preserves attributes in original #EXTINF lines.
 - Keeps broken EXTINF entries as commented lines in output so you can inspect them.
 - Removes exact duplicate URLs (keeps first).
 - Adds default EXTINF for bare URLs if present in original file.

Notes:
 - This tool is conservative: it does not fetch URLs or validate remote streams.
 - It normalizes by trimming whitespace; it treats URL uniqueness as exact string equality.
"""
from pathlib import Path
import re
import argparse
from typing import Tuple, List, Optional, Dict

RE_EXTINF = re.compile(r'^#EXTINF:(?P<payload>.*)$', flags=re.IGNORECASE)
RE_DURATION_TITLE = re.compile(r'^(?P<duration>[^,\s]+)\s*(?P<attrs>.*?)?,(?P<title>.*)$', flags=re.DOTALL)


def parse_extinf_line(line: str) -> Tuple[int, str, str, bool]:
    """
    Parse an #EXTINF line.
    Returns (duration:int, title:str, attrs:str, malformed:bool)
    malformed==True when the line couldn't be parsed into duration/title form.
    """
    m = RE_EXTINF.match(line.strip())
    if not m:
        return -1, '', '', True
    payload = m.group("payload").strip()
    m2 = RE_DURATION_TITLE.match(payload)
    if m2:
        dur_raw = m2.group("duration").strip()
        attrs = (m2.group("attrs") or "").strip()
        title = (m2.group("title") or "").strip()
        try:
            dur = int(float(dur_raw))
        except Exception:
            dur = -1
        return dur, title, attrs, False
    # fallback: try split on first comma
    if ',' in payload:
        dur_raw, title = payload.split(',', 1)
        try:
            dur = int(float(dur_raw.strip()))
        except Exception:
            dur = -1
        return dur, title.strip(), '', False
    # no comma and not parseable -> malformed
    try:
        dur = int(float(payload))
    except Exception:
        return -1, '', '', True
    return dur, '', '', False


class Entry:
    """Represents one playlist entry (maybe an EXTINF and maybe a URL)."""
    def __init__(self, extinf_line: Optional[str], ext_lineno: Optional[int], url_line: Optional[str], url_lineno: Optional[int]):
        self.extinf_line = extinf_line
        self.ext_lineno = ext_lineno
        self.url_line = url_line
        self.url_lineno = url_lineno
        # parsed fields
        self.duration = -1
        self.title = ''
        self.attrs = ''
        self.malformed_extinf = False
        if extinf_line:
            self.duration, self.title, self.attrs, self.malformed_extinf = parse_extinf_line(extinf_line)

    def normalized_url(self) -> Optional[str]:
        if not self.url_line:
            return None
        return self.url_line.strip()

    def status(self) -> str:
        if self.extinf_line and not self.url_line:
            return "broken_extinf_no_url"
        if not self.extinf_line and self.url_line:
            return "bare_url"
        if self.extinf_line and self.url_line:
            return "malformed_extinf_with_url" if self.malformed_extinf else "ok"
        return "empty"

    def to_dict(self) -> dict:
        return {
            "ext_lineno": self.ext_lineno,
            "extinf_line": self.extinf_line,
            "duration": self.duration,
            "attrs": self.attrs,
            "title": self.title,
            "malformed_extinf": self.malformed_extinf,
            "url_lineno": self.url_lineno,
            "url": self.normalized_url(),
            "status": self.status()
        }


def read_m3u(path: Path) -> Tuple[List[str], List[Entry]]:
    """Reads an M3U file and returns raw lines and a list of Entries."""
    text = path.read_text(encoding='utf-8', errors='ignore')
    lines = text.splitlines()
    entries: List[Entry] = []
    i = 0
    n = len(lines)
    while i < n:
        line = lines[i].rstrip('\n')
        stripped = line.strip()
        if stripped == '':
            i += 1
            continue
        # #EXTINF block
        if stripped.upper().startswith('#EXTINF'):
            ext_line = stripped
            ext_lineno = i + 1
            j = i + 1
            url = None
            url_lineno = None
            # find the next non-empty non-comment URL line (skip other tags)
            while j < n:
                cand = lines[j].strip()
                if cand == '':
                    j += 1
                    continue
                if cand.upper().startswith('#EXTINF'):
                    # next entry: no URL for this extinf
                    break
                if cand.startswith('#'):
                    j += 1
                    continue
                url = cand
                url_lineno = j + 1
                j += 1
                break
            entries.append(Entry(ext_line, ext_lineno, url, url_lineno))
            i = j
            continue
        # Bare URL (no preceding EXTINF)
        if not stripped.startswith('#'):
            entries.append(Entry(None, None, stripped, i + 1))
        i += 1
    return lines, entries


def write_clean(entries: List[Entry], out_path: Path, preserve_attrs: bool = True, keep_broken: bool = True, dedupe: bool = True) -> Tuple[int, int, int]:
    """
    Write cleaned playlist to out_path.
    Returns (written_entries, broken_kept_count, duplicates_removed_count).
    """
    seen = set()
    out_lines: List[str] = []
    out_lines.append('#EXTM3U')
    written = 0
    broken_kept = 0
    duplicates_removed = 0
    for e in entries:
        url = e.normalized_url()
        if url is None:
            # broken extinf with no url
            if e.extinf_line and keep_broken:
                out_lines.append(f'# BROKEN EXTINF (line {e.ext_lineno}): {e.extinf_line}')
                broken_kept += 1
            continue
        if dedupe and url in seen:
            duplicates_removed += 1
            continue
        seen.add(url)
        if e.extinf_line and preserve_attrs:
            out_lines.append(e.extinf_line)
        else:
            dur = e.duration if isinstance(e.duration, int) else -1
            title = e.title or '(no title)'
            title = title.replace('\n', ' ').strip()
            out_lines.append(f'#EXTINF:{dur},{title}')
        out_lines.append(url)
        written += 1
    out_path.write_text('\n'.join(out_lines) + '\n', encoding='utf-8')
    return written, broken_kept, duplicates_removed


def analyze_and_fix(input_file: Path, output_file: Path, preserve_attrs: bool = True, keep_broken: bool = True, dedupe: bool = True) -> Dict:
    """Analyze the file and write cleaned output. Return a summary dict and lists for inspection."""
    if not input_file.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")
    _, entries = read_m3u(input_file)
    total_entries = len(entries)
    broken = [e for e in entries if e.status() == 'broken_extinf_no_url']
    bare = [e for e in entries if e.status() == 'bare_url']
    malformed = [e for e in entries if e.status() == 'malformed_extinf_with_url']
    urls = [e.normalized_url() for e in entries if e.normalized_url() is not None]
    dup_counts = {}
    for u in urls:
        dup_counts[u] = dup_counts.get(u, 0) + 1
    exact_duplicates = {u: c for u, c in dup_counts.items() if c > 1}
    written, broken_kept, duplicates_removed = write_clean(entries, output_file, preserve_attrs=preserve_attrs, keep_broken=keep_broken, dedupe=dedupe)
    summary = {
        'total_entries': total_entries,
        'broken_count': len(broken),
        'bare_count': len(bare),
        'malformed_count': len(malformed),
        'unique_urls': len(set(urls)),
        'exact_duplicate_count': len(exact_duplicates),
        'written': written,
        'broken_kept': broken_kept,
        'duplicates_removed': duplicates_removed,
        'exact_duplicates': exact_duplicates,
    }
    # also include raw lists for optional programmatic use
    summary['_entries'] = entries
    summary['_broken'] = broken
    summary['_bare'] = bare
    summary['_malformed'] = malformed
    return summary


def main(argv=None):
    p = argparse.ArgumentParser(description='Analyze and fix M3U playlist file')
    p.add_argument('-i', '--input', required=True, help='Input .m3u or .m3u8 file')
    p.add_argument('-o', '--output', required=True, help='Cleaned output filename (.m3u)')
    p.add_argument('--no-preserve-attrs', action='store_true', help='Do not preserve original #EXTINF attributes')
    p.add_argument('--no-keep-broken', action='store_true', help='Do not keep broken EXTINF entries as commented lines')
    p.add_argument('--no-dedupe', action='store_true', help='Do not remove exact duplicate URLs')
    args = p.parse_args(argv)
    preserve = not args.no_preserve_attrs
    keep_broken = not args.no_keep_broken
    dedupe = not args.no_dedupe
    summary = analyze_and_fix(Path(args.input), Path(args.output), preserve_attrs=preserve, keep_broken=keep_broken, dedupe=dedupe)
    # Print summary for user
    print('--- Analysis summary ---')
    print('Total parsed entries:', summary['total_entries'])
    print('Broken EXTINF entries (no URL):', summary['broken_count'])
    print('Bare URL entries (no EXTINF):', summary['bare_count'])
    print('Malformed EXTINF-with-URL entries:', summary['malformed_count'])
    print('Unique URLs:', summary['unique_urls'])
    print('Exact duplicate URLs found:', summary['exact_duplicate_count'])
    print('Entries written to output:', summary['written'])
    print('Broken entries kept (commented):', summary['broken_kept'])
    print('Exact duplicate entries removed:', summary['duplicates_removed'])
    if summary['exact_duplicates']:
        print('\nSample duplicates (url => count):')
        for u, c in list(summary['exact_duplicates'].items())[:20]:
            print(f'  {u}  => {c}')
    print('\nCleaned output written to:', Path(args.output).absolute())
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
