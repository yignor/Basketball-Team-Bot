#!/bin/bash
# Обновление бота с GitHub и перезапуск
# Запуск: sudo bash /opt/basketball-bot/deploy/update.sh

set -e
BOT_DIR="/opt/basketball-bot"
SERVICE="basketball-bot"

echo "Обновление бота..."
cd "$BOT_DIR"
git pull origin master

echo "Обновление зависимостей..."
sudo -u botuser "$BOT_DIR/venv/bin/pip" install -r requirements-github.txt -q

echo "Перезапуск сервиса..."
systemctl restart "$SERVICE"
systemctl status "$SERVICE" --no-pager

echo "Готово. Логи: journalctl -u $SERVICE -f"
