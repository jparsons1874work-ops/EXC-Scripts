#!/usr/bin/env bash
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

apt update
apt install -y \
  git \
  python3 \
  python3-venv \
  python3-pip \
  curl \
  ca-certificates

echo "Base packages installed."
echo "If Selenium/Chrome scripts are required, install browser packages for your Ubuntu image."
echo "Common option: apt install -y chromium-browser chromium-chromedriver"
echo "Install nginx later with: apt install -y nginx"
