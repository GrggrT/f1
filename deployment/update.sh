#!/bin/bash
set -e

cd /home/ubuntu/f1-fantasy-bot

echo "=== Updating F1 Fantasy Bot ==="

git pull
source .venv/bin/activate
pip install -r requirements.txt

sudo systemctl restart f1bot

echo "=== Update complete! ==="
sudo systemctl status f1bot --no-pager
