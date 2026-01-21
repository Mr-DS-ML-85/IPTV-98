#!/usr/bin/env python3

import sys
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

MAX_THREADS = 220


def read_m3u(path: Path):
    try:
        lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    except Exception as e:
        print(f"[ERROR] Failed to read {path}: {e}")
        return []

    out = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        if line.upper() == "#EXTM3U":
            continue
        out.append(line)

    print(f"[OK] Loaded {path.name} ({len(out)} lines)")
    return out


def main():
    if len(sys.argv) < 4:
        print("Usage:")
        print("  python3 m3u-combine-threaded.py input1.m3u input2.m3u [inputN.m3u ...] output.m3u")
        sys.exit(1)

    *inputs, output = sys.argv[1:]
    output = Path(output)

    combined = ["#EXTM3U"]

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = {}
        for inp in inputs:
            p = Path(inp)
            if not p.exists() or not p.is_file():
                print(f"[SKIP] {inp} not found")
                continue
            futures[executor.submit(read_m3u, p)] = p.name

        for future in as_completed(futures):
            combined.extend(future.result())

    output.write_text("\n".join(combined) + "\n", encoding="utf-8")
    print(f"\n[DONE] Combined playlist saved as: {output}")
    print(f"[INFO] Total lines written (excluding header): {len(combined) - 1}")


if __name__ == "__main__":
    main()
