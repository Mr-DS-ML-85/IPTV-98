#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
super_m3u_cleaner.py

Single-file improved validator combining:
 - positional inputs (local paths or URLs) without argparse
 - asyncio + aiohttp validator with per-host limits
 - HEAD -> GET-with-Range probing
 - HLS parsing and multi-segment checks
 - scoring & CSV diagnostics
 - atomic M3U output (preserves order)
 - small, explicit CLI flags parsed from sys.argv (no argparse)
"""

from __future__ import annotations
import sys
import os
import asyncio
import aiohttp
import tempfile
import random
import time
import csv
from typing import List, Dict, Optional, Tuple
from urllib.parse import urlparse, urljoin

# -----------------------
# DEFAULT CONFIG (edit at top)
# -----------------------
DEFAULT_INPUTS = ["collect/adult_links.m3u"]
DEFAULT_OUTPUT = "final/playtv.m3u"
DEFAULT_CSV = None  # if None -> output_path + ".csv"
DEFAULT_TIMEOUT = 7.0
DEFAULT_GLOBAL_CONCURRENCY = 3000
DEFAULT_PER_HOST = 400
DEFAULT_RETRIES = 1
DEFAULT_MIN_BYTES = 32 * 1024           # 32 KB
DEFAULT_SEGMENTS_TO_CHECK = 2
DEFAULT_MIN_THROUGHPUT = 5 * 1024       # bytes/sec
DEFAULT_MODE = "accuracy"               # or "speed"
DEBUG = True
# -----------------------

VALID_CT_HINTS = ("video/", "application/vnd.apple.mpegurl", "application/x-mpegurl", "application/mpegurl", "application/octet-stream", "audio/", "text/plain")
VALID_EXTS = (".ts", ".m3u8", ".mp4", ".m3u", ".mkv")

# -----------------------
# Simple CLI parsing (no argparse)
# usage:
#   python super_m3u_cleaner.py [inputs...] [output_m3u?] [--csv path.csv] [--mode accuracy|speed] [--concurrency N] [--per-host N] [--retries N] [--timeout N] [--min-bytes N] [--segments N] [--no-ssl-verify] [--debug]
# If the last positional arg looks like an m3u file path (endswith .m3u/.m3u8) and there are two positional args, interpreter treats first as input and second as output.
# -----------------------
def parse_cli(argv: List[str]):
    # returns dict with keys: inputs (list), output (str), csv (str|None), config overrides...
    args = list(argv)
    opts = {
        "inputs": [],
        "output": DEFAULT_OUTPUT,
        "csv": DEFAULT_CSV,
        "timeout": DEFAULT_TIMEOUT,
        "concurrency": DEFAULT_GLOBAL_CONCURRENCY,
        "per_host": DEFAULT_PER_HOST,
        "retries": DEFAULT_RETRIES,
        "min_bytes": DEFAULT_MIN_BYTES,
        "segments": DEFAULT_SEGMENTS_TO_CHECK,
        "min_throughput": DEFAULT_MIN_THROUGHPUT,
        "mode": DEFAULT_MODE,
        "verify_ssl": True,
        "debug": DEBUG,
    }
    i = 0
    # collect positional until first --option
    while i < len(args):
        a = args[i]
        if not a.startswith("--"):
            opts["inputs"].append(a)
            i += 1
            continue
        # option
        if a == "--csv":
            i += 1; opts["csv"] = args[i] if i < len(args) else None; i += 1; continue
        if a == "--mode":
            i += 1; opts["mode"] = args[i] if i < len(args) else opts["mode"]; i += 1; continue
        if a == "--concurrency":
            i += 1; opts["concurrency"] = int(args[i]); i += 1; continue
        if a == "--per-host":
            i += 1; opts["per_host"] = int(args[i]); i += 1; continue
        if a == "--retries":
            i += 1; opts["retries"] = int(args[i]); i += 1; continue
        if a == "--timeout":
            i += 1; opts["timeout"] = float(args[i]); i += 1; continue
        if a == "--min-bytes":
            i += 1; opts["min_bytes"] = int(args[i]); i += 1; continue
        if a == "--segments":
            i += 1; opts["segments"] = int(args[i]); i += 1; continue
        if a == "--min-throughput":
            i += 1; opts["min_throughput"] = int(args[i]); i += 1; continue
        if a == "--no-ssl-verify":
            opts["verify_ssl"] = False; i += 1; continue
        if a == "--debug":
            opts["debug"] = True; i += 1; continue
        # unknown -> skip
        print(f"[!] Unknown option: {a} (skipping)")
        i += 1

    # If user supplied two positional arguments and second looks like output path -> treat as output
    if len(opts["inputs"]) >= 2:
        # if last positional looks like an m3u target or path and not an input file (doesn't exist) we consider it output
        last = opts["inputs"][-1]
        if last.lower().endswith((".m3u", ".m3u8")) and not os.path.exists(last):
            opts["output"] = last
            opts["inputs"] = opts["inputs"][:-1]

    # fallback: if no inputs, try defaults
    if not opts["inputs"]:
        # try DEFAULT_INPUTS
        found = [fn for fn in DEFAULT_INPUTS if os.path.exists(fn)]
        if found:
            opts["inputs"] = found
        else:
            # fallback to any .m3u in cwd
            m3us = [f for f in os.listdir(".") if f.lower().endswith((".m3u", ".m3u8"))]
            if m3us:
                opts["inputs"] = m3us
            else:
                # no inputs -> use DEFAULT_INPUTS as-is (may be URL)
                opts["inputs"] = DEFAULT_INPUTS.copy()
    # if csv not set -> output + .csv
    if opts["csv"] is None:
        opts["csv"] = opts["output"] + ".csv"
    # if mode adjustments
    if opts["mode"] == "accuracy":
        opts["concurrency"] = min(opts["concurrency"], 3000)
        opts["retries"] = max(opts["retries"], 2)
        opts["per_host"] = max(opts["per_host"], 100)
    else:
        opts["concurrency"] = max(opts["concurrency"], 2500)
        opts["retries"] = min(opts["retries"], 1)
        opts["per_host"] = max(opts["per_host"], 90)
    return opts

# -----------------------
# Utilities & parsing
# -----------------------
def is_url(s: str) -> bool:
    return s.startswith("http://") or s.startswith("https://")

def parse_m3u_text(base: str, text: str) -> List[Dict]:
    """
    Mimic earlier parse_m3u: collect tuples of (metadata, absolute_url, name)
    """
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    out = []
    i = 0
    cur_meta = ""
    while i < len(lines):
        ln = lines[i]
        if ln.lower().startswith("#extinf"):
            cur_meta = ln
            i += 1
            # gather any subsequent comment lines that are not URLs
            while i < len(lines) and lines[i].startswith("#"):
                cur_meta += "\n" + lines[i]
                i += 1
            if i < len(lines) and (lines[i].startswith("http") or ":" in lines[i]):
                url = urljoin(base, lines[i])
                name = cur_meta.split(",", 1)[-1] if "," in cur_meta else url
                out.append({"metadata": cur_meta, "url": url, "name": name})
        elif ln.startswith("http"):
            # no metadata
            url = ln
            out.append({"metadata": "", "url": url, "name": url})
        i += 1
    return out

def read_local_file_lines(path: str) -> Optional[List[str]]:
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            return f.readlines()
    except Exception as e:
        return None

# -----------------------
# Async Validator Class
# -----------------------
class SuperValidator:
    def __init__(self, timeout: float, concurrency: int, per_host: int, retries: int, min_bytes: int, segments: int, min_throughput: int, verify_ssl: bool, debug: bool):
        self.timeout = timeout
        self.global_sema = asyncio.Semaphore(concurrency)
        self.per_host_limit = per_host
        self.retries = retries
        self.min_bytes = min_bytes
        self.segments = segments
        self.min_throughput = min_throughput
        self.verify_ssl = verify_ssl
        self.debug = debug

        self.host_semas: Dict[str, asyncio.Semaphore] = {}
        self.host_last_call: Dict[str, float] = {}
        self.host_delay = 0.06

        self.user_agents = [
            "VLC/3.0.18 LibVLC/3.0.18",
            "Mozilla/5.0 (compatible; VLC/3.0)",
            "AppleCoreMedia/1.0.0.16F77",
            "Lavf/58.45.100",
        ]
        self.session: Optional[aiohttp.ClientSession] = None

    async def ensure_session(self):
        if self.session:
            return
        conn = aiohttp.TCPConnector(limit=0, force_close=False, ssl=self.verify_ssl, ttl_dns_cache=300)
        timeout = aiohttp.ClientTimeout(total=None, sock_connect=self.timeout, sock_read=self.timeout)
        self.session = aiohttp.ClientSession(connector=conn, timeout=timeout)

    def host_sema(self, host: str) -> asyncio.Semaphore:
        if host not in self.host_semas:
            self.host_semas[host] = asyncio.Semaphore(self.per_host_limit)
        return self.host_semas[host]

    async def pace_host(self, host: str):
        last = self.host_last_call.get(host)
        if last:
            elapsed = time.monotonic() - last
            if elapsed < self.host_delay:
                await asyncio.sleep(self.host_delay - elapsed)
        self.host_last_call[host] = time.monotonic()

    def looks_like_hls(self, url: str, ct: str = "") -> bool:
        return url.lower().endswith(".m3u8") or ("mpegurl" in (ct or "").lower())

    def looks_like_media_ext(self, url: str) -> bool:
        return url.split("?",1)[0].lower().endswith(VALID_EXTS)

    async def probe(self, url: str, name: str) -> Dict:
        """
        Returns diagnostic dict:
        { url, valid (bool), score (0-100), confidence, reason, metrics: {...} }
        """
        await self.ensure_session()
        host = urlparse(url).netloc.lower()
        sem = self.host_sema(host)
        attempt = 0
        reason = "no-attempt"
        metrics = {"attempts":0, "status": None, "content_type": "", "bytes_read":0, "duration_s":0.0, "throughput_bps":0.0, "segments_checked":0, "segments_ok":0, "html":False}
        while attempt <= self.retries:
            attempt += 1
            metrics["attempts"] = attempt
            try:
                async with self.global_sema, sem:
                    await self.pace_host(host)
                    ua = random.choice(self.user_agents)
                    headers = {"User-Agent": ua, "Accept": "*/*"}
                    # Try HEAD first
                    try:
                        async with self.session.head(url, headers=headers, allow_redirects=True) as hr:
                            status = hr.status
                            ct = hr.headers.get("Content-Type", "") or ""
                            metrics["status"] = status
                            metrics["content_type"] = ct
                            if 200 <= status < 300 and any(h in ct.lower() for h in VALID_CT_HINTS):
                                # quick positive but still do a small GET probe to ensure bytes are served
                                pass
                    except (aiohttp.ClientResponseError, aiohttp.ClientError, asyncio.TimeoutError):
                        # fall through to GET
                        pass

                    # GET with Range
                    get_headers = headers.copy()
                    get_headers["Range"] = f"bytes=0-{max(self.min_bytes, 65535)}"
                    start = time.monotonic()
                    try:
                        async with self.session.get(url, headers=get_headers, allow_redirects=True) as r:
                            status = r.status
                            ct = r.headers.get("Content-Type", "") or ""
                            metrics["status"] = status
                            metrics["content_type"] = ct
                            # read limited data
                            data = await self._read_limited(r, self.min_bytes, 30)
                            duration = time.monotonic() - start
                            metrics["duration_s"] = duration
                            metrics["bytes_read"] = len(data)
                            metrics["throughput_bps"] = int(len(data) / (duration or 0.0001))
                            # HTML detect
                            sample = data[:512].lower()
                            if b"<html" in sample or b"<!doctype" in sample or b"<body" in sample:
                                metrics["html"] = True
                                reason = f"HTML_page({status})"
                                return self._build_result(url, False, 0, reason, metrics)
                            # HLS playlist
                            if self.looks_like_hls(url, ct) or url.lower().endswith(".m3u8"):
                                # decode text if possible
                                try:
                                    text = data.decode(errors="ignore")
                                except Exception:
                                    text = await r.text(errors="ignore")
                                ok, seg_metrics, seg_reason = await self._probe_hls_segments(url, text)
                                metrics.update(seg_metrics)
                                if ok:
                                    score = self._score(metrics, hls_bonus=20)
                                    return self._build_result(url, True, score, seg_reason, metrics)
                                else:
                                    reason = "hls_segments_failed"
                                    # let retries happen or final false
                            else:
                                # direct stream candidates
                                if (200 <= status < 300 or status == 206) and metrics["bytes_read"] >= self.min_bytes:
                                    score = self._score(metrics)
                                    reason = f"GET_{status}_bytes={metrics['bytes_read']}"
                                    return self._build_result(url, True, score, reason, metrics)
                                # if small bytes but good hint
                                if (200 <= status < 300 or status == 206) and (any(h in ct.lower() for h in VALID_CT_HINTS) or self.looks_like_media_ext(url)):
                                    if metrics["throughput_bps"] >= self.min_throughput and metrics["bytes_read"] > (self.min_bytes // 4):
                                        score = self._score(metrics)
                                        reason = "low-bytes-but-throughput-ok"
                                        return self._build_result(url, True, score, reason, metrics)
                                    else:
                                        reason = "low-bytes-low-throughput"
                    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                        reason = f"network:{type(e).__name__}"
            except asyncio.CancelledError:
                raise
            # backoff before retry
            if attempt <= self.retries:
                await asyncio.sleep((0.35 * (2 ** (attempt - 1))) + random.random() * 0.2)
        # exhausted
        return self._build_result(url, False, 0, reason, metrics)

    async def _probe_hls_segments(self, playlist_url: str, playlist_text: str) -> Tuple[bool, Dict, str]:
        seg_metrics = {"segments_checked":0, "segments_ok":0, "segment_bytes":0, "segment_duration_s":0.0, "segment_avg_throughput_bps":0}
        lines = [ln.strip() for ln in playlist_text.splitlines() if ln.strip() and not ln.strip().startswith("#")]
        if not lines:
            return False, seg_metrics, "no_segments"
        checked = 0
        total_bytes = 0
        total_time = 0.0
        for line in lines:
            if checked >= self.segments:
                break
            seg = line
            if not seg.startswith("http"):
                seg = urljoin(playlist_url, seg)
            try:
                ua = random.choice(self.user_agents)
                headers = {"User-Agent": ua, "Accept": "*/*", "Range": f"bytes=0-{max(self.min_bytes,65535)}"}
                start = time.monotonic()
                async with self.session.get(seg, headers=headers, allow_redirects=True) as r:
                    status = r.status
                    if not (200 <= status < 300 or status == 206):
                        continue
                    data = await self._read_limited(r, self.min_bytes, 20)
                    duration = time.monotonic() - start
                    if b"<html" in (data[:512].lower()):
                        continue
                    blen = len(data)
                    if blen == 0:
                        continue
                    checked += 1
                    total_bytes += blen
                    total_time += duration
                    seg_metrics["segments_checked"] = checked
                    seg_metrics["segment_bytes"] = total_bytes
                    seg_metrics["segment_duration_s"] = total_time
                    seg_metrics["segment_avg_throughput_bps"] = int(total_bytes / (total_time or 0.0001))
                    if blen >= self.min_bytes or (blen > (self.min_bytes // 4) and (blen / max(duration,0.001) >= self.min_throughput)):
                        seg_metrics["segments_ok"] += 1
            except Exception:
                continue
        # need at least one OK to accept
        if seg_metrics["segments_ok"] >= 1:
            reason = f"segments_ok={seg_metrics['segments_ok']}/{seg_metrics['segments_checked']}"
            return True, seg_metrics, reason
        return False, seg_metrics, "segments_failed"

    async def _read_limited(self, response: aiohttp.ClientResponse, limit: int, chunk_limit:int=20) -> bytes:
        data = bytearray()
        tries = 0
        while len(data) < limit and tries < chunk_limit:
            try:
                chunk = await response.content.read(8192)
            except (asyncio.TimeoutError, aiohttp.ClientError):
                break
            if not chunk:
                break
            data.extend(chunk)
            tries += 1
        return bytes(data)

    def _score(self, metrics: Dict, hls_bonus:int=0) -> int:
        score = 0
        st = metrics.get("status") or 0
        if 200 <= st < 300:
            score += 20
        if st == 206:
            score += 15
        ct = (metrics.get("content_type") or "").lower()
        if any(k in ct for k in VALID_CT_HINTS):
            score += 15
        if metrics.get("html"):
            score -= 40
        b = metrics.get("bytes_read", 0)
        if b >= self.min_bytes:
            score += 25
        elif b >= (self.min_bytes // 4):
            score += 8
        tp = metrics.get("throughput_bps", 0)
        if tp >= self.min_throughput:
            score += 10
        score += hls_bonus
        return max(0, min(100, int(score)))

    def _build_result(self, url: str, valid: bool, score: int, reason: str, metrics: Dict) -> Dict:
        conf = "high" if score >= 70 else ("medium" if score >= 40 else "low")
        return {"url": url, "valid": valid, "score": score, "confidence": conf, "reason": reason, "metrics": metrics}

    async def close(self):
        if self.session:
            await self.session.close()
            self.session = None

# -----------------------
# High-level runner utilities
# -----------------------
async def validate_items_and_write(items: List[Dict], opts: dict):
    validator = SuperValidator(
        timeout=opts["timeout"],
        concurrency=opts["concurrency"],
        per_host=opts["per_host"],
        retries=opts["retries"],
        min_bytes=opts["min_bytes"],
        segments=opts["segments"],
        min_throughput=opts["min_throughput"],
        verify_ssl=opts["verify_ssl"],
        debug=opts["debug"],
    )
    try:
        await validator.ensure_session()
        # schedule probes preserving index
        results = [None] * len(items)
        async def worker(it):
            idx = it["index"]
            diag = await validator.probe(it["url"], it.get("name") or it["url"])
            diag["index"] = idx
            diag["metadata"] = it.get("metadata", "#EXTINF:-1,Unknown")
            diag["name"] = it.get("name") or it.get("metadata") or it["url"]
            results[idx] = diag
            mark = "✓" if diag["valid"] else "✗"
            print(f"[{mark}] idx={idx} score={diag['score']:3d} conf={diag['confidence']:<6} {diag['name']} -> {diag['reason']}")
        tasks = [asyncio.create_task(worker(it)) for it in items]
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            for t in tasks: t.cancel()
            raise
        # write CSV diagnostics
        csv_path = opts["csv"]
        write_csv_report(csv_path, results)
        # write M3U filtered (choose min confidence 'medium' by default)
        atomic_write_m3u(opts["output"], results, min_conf="medium")
        return results
    finally:
        await validator.close()

def write_csv_report(csv_path: str, diagnostics: List[Dict]):
    os.makedirs(os.path.dirname(csv_path) or ".", exist_ok=True)
    fieldnames = ["index","name","url","valid","score","confidence","reason","status","content_type","bytes_read","duration_s","throughput_bps","segments_checked","segments_ok"]
    with open(csv_path, "w", newline="", encoding="utf-8") as cf:
        w = csv.DictWriter(cf, fieldnames=fieldnames)
        w.writeheader()
        for d in diagnostics:
            m = d.get("metrics",{})
            w.writerow({
                "index": d.get("index"),
                "name": d.get("name"),
                "url": d.get("url"),
                "valid": d.get("valid"),
                "score": d.get("score"),
                "confidence": d.get("confidence"),
                "reason": d.get("reason"),
                "status": m.get("status"),
                "content_type": m.get("content_type"),
                "bytes_read": m.get("bytes_read"),
                "duration_s": round(m.get("duration_s",0.0),3),
                "throughput_bps": m.get("throughput_bps"),
                "segments_checked": m.get("segments_checked"),
                "segments_ok": m.get("segments_ok"),
            })
    print(f"[i] Wrote diagnostics CSV -> {csv_path}")

def atomic_write_m3u(output_path: str, diagnostics: List[Dict], min_conf: str = "medium"):
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix="m3u_tmp_", suffix=".m3u", dir=os.path.dirname(output_path) or ".")
    os.close(fd)
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            f.write("#EXTM3U\n")
            for d in diagnostics:
                conf = d.get("confidence","low")
                valid = d.get("valid", False)
                allow = valid and ((conf == "high") or (conf == "medium" and min_conf in ("medium","low")) or (conf == "low" and min_conf == "low"))
                if allow:
                    f.write(f"{d.get('metadata','#EXTINF:-1,Unknown')}\n{d.get('url')}\n")
        os.replace(tmp, output_path)
    finally:
        if os.path.exists(tmp):
            try: os.remove(tmp)
            except Exception: pass
    print(f"[i] Wrote filtered M3U -> {output_path} (min_conf={min_conf})")

# -----------------------
# Input builder: accept local m3u files or URL playlists
# -----------------------
def build_items_from_inputs(inputs: List[str]) -> List[Dict]:
    items: List[Dict] = []
    idx = 0
    for inp in inputs:
        if is_url(inp):
            # try fetch playlist text synchronously using aiohttp in event loop; here we do simple sync fallback using requests not available -> we'll treat as URL and add as single item (probe will fetch)
            # Add as single playlist entry (if it's m3u content it will be parsed during probing)
            items.append({"index": idx, "metadata":"", "url": inp, "name": inp})
            idx += 1
            continue
        # local path
        if os.path.exists(inp):
            # if file, read content
            try:
                with open(inp, "r", encoding="utf-8", errors="ignore") as f:
                    text = f.read()
                # quick check if m3u like
                if "#EXTM3U" in text.upper():
                    parsed = parse_m3u_text(inp, text)
                    for p in parsed:
                        p["index"] = idx
                        items.append(p)
                        idx += 1
                    continue
                else:
                    # not a playlist but maybe raw URL list: lines with http
                    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
                    for ln in lines:
                        if ln.startswith("http"):
                            items.append({"index": idx, "metadata":"", "url": ln, "name": ln})
                            idx += 1
                    continue
            except Exception:
                # fallback: treat as single URL string
                items.append({"index": idx, "metadata":"", "url": inp, "name": inp})
                idx += 1
                continue
        # non-existing local file -> treat as URL (user may have given a remote path)
        items.append({"index": idx, "metadata":"", "url": inp, "name": inp})
        idx += 1
    return items

# -----------------------
# main
# -----------------------
def main(argv):
    opts = parse_cli(argv)
    # print summary
    print("[i] super_m3u_cleaner starting")
    print(f"[i] inputs: {opts['inputs']}")
    print(f"[i] output: {opts['output']}  csv: {opts['csv']}")
    print(f"[i] mode={opts['mode']} concurrency={opts['concurrency']} per_host={opts['per_host']} retries={opts['retries']} timeout={opts['timeout']}")
    # build items
    items = build_items_from_inputs(opts["inputs"])
    if not items:
        print("[!] No playlist items found. Exiting.")
        return 1
    print(f"[i] total parsed items: {len(items)}")
    # run event loop validate
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    try:
        results = loop.run_until_complete(validate_items_and_write(items, opts))
    except KeyboardInterrupt:
        print("\n[!] Interrupted by user")
        return 2
    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
