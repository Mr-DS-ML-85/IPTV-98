#!/usr/bin/env bash
set -e

# Go to the script directory to make relative paths predictable
cd "$(dirname "$0")"

# 1) Run the pipeline
bash install_libs.sh
./health.sh
python3 fetch.py
python3 ultra-iptv.py
python3 rm-dupe.py collect/playlist.m3u
python3 url_checker.py
python m3u_merger.py collect/play.m3u collect/TV.m3u playtv.m3u --threads=300
# validate everything in collect, write to final, use ffprobe
echo "Scraping and building m3u file done successfully filename: playtv.m3u in final"

# 2) Post‑processing:
# 2a) Clean collect folder

# 2b) Move final/playtv.m3u to ../playlist, replacing old one
if [ -f final/play.m3u ]; then
    rm -f ../playlist/playtv.m3u
    mv final/play.m3u ../playlist/playtv.m3u
    echo "Moved new playtv.m3u to playlist/"
else
    echo "ERROR: final/playtv.m3u not found, nothing to move" >&2
    exit 1
fi


