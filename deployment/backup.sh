#!/bin/bash
set -e

BACKUP_DIR="/home/ubuntu/f1-fantasy-bot/backups"
DB_PATH="/home/ubuntu/f1-fantasy-bot/data/fantasy.db"
MAX_BACKUPS=7

mkdir -p "$BACKUP_DIR"

if [ ! -f "$DB_PATH" ]; then
    echo "Database not found at $DB_PATH"
    exit 1
fi

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
cp "$DB_PATH" "$BACKUP_DIR/fantasy_${TIMESTAMP}.db"
echo "Backup created: fantasy_${TIMESTAMP}.db"

# Rotate: keep only last N backups
cd "$BACKUP_DIR"
ls -t fantasy_*.db | tail -n +$((MAX_BACKUPS + 1)) | xargs -r rm --
echo "Kept last $MAX_BACKUPS backups"
