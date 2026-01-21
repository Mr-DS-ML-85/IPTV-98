#!/usr/bin/env bash
set -e

# Go to the script directory to make relative paths predictable
cd "$(dirname "$0")"

# 1) Run the pipeline
python3 ultra-iptv.py
python3 rm-dupe.py collect/
python3 m3u-vaildator.py collect/playlist_dedup.m3u
# validate everything in collect, write to final, use ffprobe
python3 vaildator.py collect --outdir final --ffprobe
python3 merge_playlists.py collect/playtv.m3u collect/working.m3u collect/maybe.m3u --output final/playtv.m3u
python3 m3u-dedupe.py final/
rm -rf playtv.m3u
cp playtv_dedup.m3u playtv.m3u
rm -rf playtv_dedup.m3u
echo "Scraping and building m3u file done successfully filename: playtv.m3u in final"

# 2) Post‑processing:
# 2a) Clean collect folder
rm -f collect/*

# 2b) Move final/playtv.m3u to ../playlist, replacing old one
if [ -f final/playtv.m3u ]; then
    rm -f ../playlist/playtv.m3u
    mv final/playtv.m3u ../playlist/playtv.m3u
    echo "Moved new playtv.m3u to playlist/"
else
    echo "ERROR: final/playtv.m3u not found, nothing to move" >&2
    exit 1
fi


