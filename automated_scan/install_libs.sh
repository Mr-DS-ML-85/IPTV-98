#!/usr/bin/env bash
set -e

echo "Updating package lists..."
sudo apt-get update -y

echo "Installing required system libraries for Playwright and your IPTV scraper..."
sudo apt-get install -y \
  libgtk-4-1 \
  libgraphene-1.0-0 \
  libwoff2-1 \
  libvpx9 \
  libevent-2.1-7 \
  libopus0 \
  gstreamer1.0-plugins-base \
  gstreamer1.0-plugins-good \
  gstreamer1.0-plugins-bad \
  gstreamer1.0-plugins-ugly \
  gstreamer1.0-libav \
  gstreamer1.0-tools \
  flite \
  libavif16 \
  libharfbuzz-icu0 \
  libsecret-1-0 \
  libhyphen0 \
  libgles2-mesa \
  libx264-163 \
  wget curl unzip \
  fonts-liberation \
  libnss3 \
  libxss1 \
  libasound2 \
  libatk-bridge2.0-0 \
  libgtk-3-0 \
  libxcomposite1 \
  libxcursor1 \
  libxdamage1 \
  libxi6 \
  libxtst6 \
  libxrandr2 \
  libgbm1

echo "All required system libraries are installed."

# Optional: Install Python packages and Playwright browsers
playwright install
# echo "Playwright browsers installed."
