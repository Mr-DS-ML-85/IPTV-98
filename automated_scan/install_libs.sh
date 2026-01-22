#!/usr/bin/env bash
set -e

echo "--- Updating apt lists ---"
sudo apt-get update -y

echo "--- Installing Playwright system dependencies ---"
python3 -m playwright install --with-deps
echo "✔ System dependencies installed."
echo "Now run: python -m playwright install"
bash health.sh
