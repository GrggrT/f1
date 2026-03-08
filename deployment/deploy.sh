#!/bin/bash
set -e

echo "=== F1 Fantasy Bot — Deploy ==="

# Install system deps
sudo apt update && sudo apt install -y python3.12 python3.12-venv python3-pip git

# Clone or pull
REPO_DIR="/home/ubuntu/f1-fantasy-bot"
if [ -d "$REPO_DIR" ]; then
    echo "Repo exists, pulling latest..."
    cd "$REPO_DIR" && git pull
else
    echo "Cloning repo..."
    git clone https://github.com/GrggrT/f1.git "$REPO_DIR"
    cd "$REPO_DIR"
fi

# Venv
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# .env
if [ ! -f .env ]; then
    cp .env.example .env
    echo ""
    echo ">>> EDIT .env with your settings:"
    echo ">>>   nano $REPO_DIR/.env"
    echo ">>> Then run: sudo systemctl restart f1bot"
    echo ""
fi

# Create data directory
mkdir -p data

# systemd service
sudo tee /etc/systemd/system/f1bot.service > /dev/null << 'EOF'
[Unit]
Description=F1 Fantasy Telegram Bot
After=network.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/f1-fantasy-bot
Environment=PATH=/home/ubuntu/f1-fantasy-bot/.venv/bin
ExecStart=/home/ubuntu/f1-fantasy-bot/.venv/bin/python bot.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable f1bot
sudo systemctl start f1bot

echo ""
echo "=== Deploy complete! ==="
echo "Status: sudo systemctl status f1bot"
echo "Logs:   sudo journalctl -u f1bot -f"
