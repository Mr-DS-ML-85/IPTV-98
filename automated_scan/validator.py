#!/usr/bin/env python3
"""
m3u-vaildator.py  (note: matches your repo filename)

Place in: IPTV-98/automated_scan/m3u-vaildator.py

Usage examples (from repo root or automated_scan/):
    # validate everything in collect (default)
    python3 automated_scan/m3u-vaildator.py

    # validate a single playlist
    python3 automated_scan/m3u-vaildator.py automated_scan/collect/playtv.m3u

    # custom outdir and ffprobe
    python3 automated_scan/m3u-vaildator.py automated_scan/collect --outdir automated_scan/final --ffprobe --concurrency 30

Notes:
 - This script NEVER modifies files in collect/.
 - Outputs are written to the outdir (default: automated_scan/final).
 - Requires Python 3.8+. Optional ffprobe (system package) can be enabled with --ffprobe.
 - Install deps for network: pip install aiohttp async-timeout
"""
from __future__ import annotations
import argparse
import asyncio
import async_timeout
import aiohttp
import shutil
import subprocess
import random
import re
import time
from pathlib import Path
from typing import Optional

# ---------------------------
# Defaults / Tunables
# ---------------------------
DEFAULT_INPUT = "automated_scan/collect"
DEFAULT_OUTDIR = "automated_scan/final"
DEFAULT_CONCURRENCY = 200
DEFAULT_TIMEOUT_HEAD = 39
DEFAULT_TIMEOUT_GET = 30
DEFAULT_RETRIES = 3
RATE_DELAY_MIN = 0.15
RATE_DELAY_MAX = 0.45

GOOD_CT_RE = re.compile(r"(mpegurl|application/x-mpegurl|application/vnd\.apple\.mpegurl|video|audio)", re.I)
HTML_BYTES = b"<html"
EXTM3U_BYTES = b"#EXTM3U"

# ---------------------------
# Utilities
# ---------------------------
def has_ffprobe() -> bool:
    return shutil.which("ffprobe") is not None

def ffprobe_check(url: str, timeout: int = 12) -> bool:
    """
    Run a light ffprobe to check for a playable stream.
    Blocking; intended to be run in executor.
    """
    if not has_ffprobe():
        return False
    cmd = [
        "ffprobe",
        "-v", "error",
        "-select_streams", "v:0",
        "-show_entries", "stream=codec_name",
        "-of", "default=nw=1:nk=1",
        url,
    ]
    try:
        p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
        ok = (p.returncode == 0 and p.stdout.strip() != b"")
        return ok
    except Exception:
        return False

# ---------------------------
# M3U parsing
# ---------------------------
def parse_m3u_file(path: Path):
    """
    Yields (extinf_line_or_None, url)
    Keeps the last seen #EXTINF for the following URL.
    """
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return
    ext = None
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("#EXTINF"):
            ext = line
            continue
        if line.startswith("#"):
            # skip other comment lines
            continue
        yield ext, line
        ext = None

# ---------------------------
# Network helpers (async)
# ---------------------------
async def safe_head(session: aiohttp.ClientSession, url: str, timeout: int) -> Optional[aiohttp.ClientResponse]:
    try:
        async with async_timeout.timeout(timeout):
            resp = await session.request("HEAD", url, allow_redirects=True)
            return resp
    except Exception:
        return None

async def safe_get_bytes(session: aiohttp.ClientSession, url: str, timeout: int, max_bytes: int) -> Optional[bytes]:
    try:
        async with async_timeout.timeout(timeout):
            async with session.get(url, allow_redirects=True) as resp:
                chunk = await resp.content.read(max_bytes)
                return chunk
    except Exception:
        return None

# ---------------------------
# Classification logic
# ---------------------------
async def classify_stream(session: aiohttp.ClientSession, url: str, use_ffprobe: bool,
                          timeout_head: int = DEFAULT_TIMEOUT_HEAD, timeout_get: int = DEFAULT_TIMEOUT_GET) -> str:
    """
    Returns one of: "working", "maybe", "broken"
    Strategy:
      1) HEAD quick check (ct/200 -> working; HTML -> broken)
      2) GET small bytes and inspect for EXT M3U or HTML
      3) Retry HEAD/GET for confirmation
      4) Optionally call ffprobe (blocking in executor)
    """
    # HEAD quick
    try:
        resp = await safe_head(session, url, timeout_head)
    except Exception:
        resp = None

    if resp is not None:
        status = getattr(resp, "status", None)
        if status == 200:
            ct = resp.headers.get("Content-Type", "")
            if ct and GOOD_CT_RE.search(ct):
                return "working"
            if "html" in (ct or "").lower():
                return "broken"

    # GET small bytes
    try:
        chunk = await safe_get_bytes(session, url, timeout_get, max_bytes=2048)
        if chunk:
            low = chunk.lower()
            if EXTM3U_BYTES in low:
                return "working"
            if HTML_BYTES in low:
                return "broken"
            if len(chunk) > 32:
                # ambiguous, escalate to ffprobe if available
                if use_ffprobe:
                    loop = asyncio.get_event_loop()
                    ok = await loop.run_in_executor(None, ffprobe_check, url, 12)
                    return "working" if ok else "maybe"
                return "maybe"
    except Exception:
        pass

    # confirm retries
    for _ in range(DEFAULT_RETRIES):
        try:
            resp2 = await safe_head(session, url, timeout_head * 2)
            if resp2 and getattr(resp2, "status", None) == 200:
                ct2 = resp2.headers.get("Content-Type", "")
                if ct2 and GOOD_CT_RE.search(ct2):
                    return "working"
                if "html" in (ct2 or "").lower():
                    return "broken"
            chunk2 = await safe_get_bytes(session, url, timeout_get * 2, max_bytes=4096)
            if chunk2:
                low2 = chunk2.lower()
                if EXTM3U_BYTES in low2:
                    return "working"
                if HTML_BYTES in low2:
                    return "broken"
                if len(chunk2) > 64:
                    if use_ffprobe:
                        loop = asyncio.get_event_loop()
                        ok = await loop.run_in_executor(None, ffprobe_check, url, 12)
                        return "working" if ok else "maybe"
                    return "maybe"
        except Exception:
            pass
        await asyncio.sleep(0.4 + random.random() * 0.4)

    # final ffprobe attempt
    if use_ffprobe:
        loop = asyncio.get_event_loop()
        ok = await loop.run_in_executor(None, ffprobe_check, url, 12)
        return "working" if ok else "broken"

    return "broken"

# ---------------------------
# Per-file processing + aggregated outputs
# ---------------------------
async def process_file(session: aiohttp.ClientSession, src: Path, outdir: Path, agg_files: dict,
                       sem: asyncio.Semaphore, use_ffprobe: bool,
                       timeout_head: int, timeout_get: int):
    name = src.stem
    target_dir = outdir / name
    target_dir.mkdir(parents=True, exist_ok=True)

    f_ok = (target_dir / "working.m3u").open("w", encoding="utf-8")
    f_maybe = (target_dir / "maybe.m3u").open("w", encoding="utf-8")
    f_broken = (target_dir / "broken.m3u").open("w", encoding="utf-8")

    total = 0
    counts = {"working": 0, "maybe": 0, "broken": 0}

    coros = []
    for ext, url in parse_m3u_file(src):
        total += 1
        async def check_and_return(e=ext, u=url):
            async with sem:
                await asyncio.sleep(random.uniform(RATE_DELAY_MIN, RATE_DELAY_MAX))
                try:
                    status = await classify_stream(session, u, use_ffprobe, timeout_head, timeout_get)
                except Exception:
                    status = "broken"
                return (e, u, status)
        coros.append(check_and_return())

    for task in asyncio.as_completed(coros):
        ext, url, status = await task
        counts[status] += 1
        if status == "working":
            if ext:
                f_ok.write(ext + "\n")
                agg_files["working"].write(ext + "\n")
            f_ok.write(url + "\n")
            agg_files["working"].write(url + "\n")
        elif status == "maybe":
            if ext:
                f_maybe.write(ext + "\n")
                agg_files["maybe"].write(ext + "\n")
            f_maybe.write(url + "\n")
            agg_files["maybe"].write(url + "\n")
        else:
            if ext:
                f_broken.write(ext + "\n")
                agg_files["broken"].write(ext + "\n")
            f_broken.write(url + "\n")
            agg_files["broken"].write(url + "\n")

    f_ok.close(); f_maybe.close(); f_broken.close()
    print(f"[INFO] {src.name}: total={total} working={counts['working']} maybe={counts['maybe']} broken={counts['broken']}")

async def validate_paths(paths: list[Path], outdir: Path, concurrency: int,
                         use_ffprobe: bool, timeout_head: int, timeout_get: int):
    outdir.mkdir(parents=True, exist_ok=True)
    agg_ok = (outdir / "aggregated_working.m3u").open("w", encoding="utf-8")
    agg_maybe = (outdir / "aggregated_maybe.m3u").open("w", encoding="utf-8")
    agg_broken = (outdir / "aggregated_broken.m3u").open("w", encoding="utf-8")
    agg_files = {"working": agg_ok, "maybe": agg_maybe, "broken": agg_broken}

    timeout = aiohttp.ClientTimeout(total=None, sock_connect=10, sock_read=15)
    sem = asyncio.Semaphore(concurrency)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = [process_file(session, p, outdir, agg_files, sem, use_ffprobe, timeout_head, timeout_get) for p in paths]
        await asyncio.gather(*tasks)

    for f in agg_files.values():
        f.close()

# ---------------------------
# CLI + helpers
# ---------------------------
def collect_input_paths(inp: Path) -> list[Path]:
    if inp.is_file() and inp.suffix.lower() in (".m3u", ".m3u8"):
        return [inp]
    if inp.is_dir():
        files = sorted([p for p in inp.glob("*.m3u")] + [p for p in inp.glob("*.m3u8")])
        return files
    raise SystemExit(f"Input must be a .m3u/.m3u8 file or a directory containing such files: {inp}")

def cli():
    p = argparse.ArgumentParser(prog="m3u-vaildator.py", description="Validate .m3u/.m3u8 streams")
    p.add_argument("input", nargs="?", default=DEFAULT_INPUT, help="Path to .m3u file or directory (default: automated_scan/collect)")
    p.add_argument("--outdir", default=None, help="Output directory (default: automated_scan/final)")
    p.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY, help="Concurrent checks (default 20)")
    p.add_argument("--ffprobe", action="store_true", help="Enable ffprobe final confirmation (requires ffprobe in PATH)")
    p.add_argument("--timeout-head", type=int, default=DEFAULT_TIMEOUT_HEAD, help="HEAD timeout (sec)")
    p.add_argument("--timeout-get", type=int, default=DEFAULT_TIMEOUT_GET, help="GET timeout (sec)")
    p.add_argument("--version", action="version", version="m3u-vaildator 1.0")
    return p.parse_args()

def main():
    args = cli()
    inp = Path(args.input).expanduser().resolve()
    if not inp.exists():
        raise SystemExit(f"Input path not found: {inp}")

    # default outdir logic: if user didn't pass outdir, use automated_scan/final when possible
    if args.outdir:
        outdir = Path(args.outdir).expanduser().resolve()
    else:
        # if input is automated_scan/collect use automated_scan/final
        parent = inp if inp.is_dir() else inp.parent
        if parent.name == "collect" and parent.parent.joinpath("final").exists():
            outdir = parent.parent / "final"
        else:
            outdir = Path(DEFAULT_OUTDIR)
    outdir.mkdir(parents=True, exist_ok=True)

    if args.ffprobe and not has_ffprobe():
        print("[WARN] --ffprobe requested but ffprobe not found. Continuing without ffprobe.")
        args.ffprobe = False

    paths = collect_input_paths(inp)
    if not paths:
        print("[INFO] No .m3u files found under input.")
        return

    print(f"[START] Validating {len(paths)} file(s). Outdir: {outdir}")
    start = time.time()
    try:
        asyncio.run(validate_paths(paths, outdir, args.concurrency, args.ffprobe, args.timeout_head, args.timeout_get))
    except KeyboardInterrupt:
        print("[!] Interrupted by user")
    elapsed = time.time() - start
    print(f"[DONE] Elapsed {elapsed:.1f}s. Results in: {outdir}")

if __name__ == "__main__":
    main()
