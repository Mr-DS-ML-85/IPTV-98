#!/usr/bin/env bash
set -e

# Go to the script directory to make relative paths predictable
cd "$(dirname "$0")"

# 1) Run the pipeline
python3 ultra-iptv.py
python3 rm-dupe.py collect/
python3 m3u-vaildator.py collect/playlist_dedup.m3u
# validate everything in collect, write to final, use ffprobe
bash health.sh
python3 merge_playlists.py collect/ --output final/play.m3u
echo "Scraping and building m3u file done successfully filename: playtv.m3u in final"

# 2) Post‑processing:
# 2a) Clean collect folder
rm -f collect/*

# 2b) Move final/playtv.m3u to ../playlist, replacing old one
if [ -f final/play.m3u ]; then
    rm -f ../playlist/playtv.m3u
    mv final/play.m3u ../playlist/playtv.m3u
    echo "Moved new playtv.m3u to playlist/"
else
    echo "ERROR: final/playtv.m3u not found, nothing to move" >&2
    exit 1
fi


