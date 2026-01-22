#!/usr/bin/env bash
set -e

echo "--- Updating apt lists ---"
sudo apt-get update -y

echo "--- Installing Playwright system dependencies ---"
sudo apt-get install -y \
  libgtk‑4‑1t64 \
  libgtk‑3‑0t64 \
  libasound2t64 \
  libevent‑2.1‑7t64 \
  libopus0 \
  libvpx9 \
  libharfbuzz‑icu0 \
  libsecret‑1‑0 \
  libhyphen0 \
  libxrandr2 \
  libxss1 \
  libnss3 \
  libxcomposite1 \
  libxcursor1 \
  libxdamage1 \
  libxi6 \
  libxtst6 \
  libgbm1 \
  gstreamer1.0‑plugins‑base \
  gstreamer1.0‑plugins‑good \
  gstreamer1.0‑plugins‑bad \
  gstreamer1.0‑plugins‑ugly \
  gstreamer1.0‑libav \
  wget curl unzip fonts‑liberation

echo "--- Optional: Installing woff2 (font compression) ---"
# This installs the woff2 utility package
sudo apt-get install -y woff2 || echo "woff2 not found, skipping"

echo ""
echo "✔ System dependencies installed."
echo "Now run: python -m playwright install"
python -m playwright install
