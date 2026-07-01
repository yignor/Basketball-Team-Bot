#!/bin/bash
# =============================================================================
# Установка Basketball Bot на Ubuntu
# Запуск: sudo bash setup.sh
# =============================================================================
set -e

BOT_DIR="/opt/basketball-bot"
BOT_USER="botuser"
REPO_URL="https://github.com/yignor/Basketball-Team-Bot.git"
LOG_DIR="/var/log/basketball-bot"
SERVICE_NAME="basketball-bot"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()    { echo -e "${GREEN}[✓]${NC} $1"; }
warn()    { echo -e "${YELLOW}[!]${NC} $1"; }
error()   { echo -e "${RED}[✗]${NC} $1"; exit 1; }

[ "$EUID" -ne 0 ] && error "Запустите с sudo: sudo bash setup.sh"

echo ""
echo "============================================="
echo "  Basketball Bot — установка на Ubuntu"
echo "============================================="
echo ""

# ── 1. Системные пакеты ──────────────────────────────────────────────────────
info "Обновление пакетов..."
apt-get update -qq
apt-get install -y -qq python3 python3-pip python3-venv git curl nano 2>/dev/null
info "Python $(python3 --version) установлен"

# ── 2. Системный пользователь ────────────────────────────────────────────────
if ! id "$BOT_USER" &>/dev/null; then
    useradd --system --shell /bin/bash --create-home "$BOT_USER"
    info "Пользователь $BOT_USER создан"
else
    info "Пользователь $BOT_USER уже существует"
fi

# ── 3. Папка и репозиторий ───────────────────────────────────────────────────
if [ -d "$BOT_DIR/.git" ]; then
    info "Репозиторий уже клонирован, обновляем..."
    cd "$BOT_DIR" && git pull
else
    info "Клонируем репозиторий..."
    git clone "$REPO_URL" "$BOT_DIR"
fi
chown -R "$BOT_USER:$BOT_USER" "$BOT_DIR"

# ── 4. Python окружение ──────────────────────────────────────────────────────
info "Создание virtualenv..."
sudo -u "$BOT_USER" python3 -m venv "$BOT_DIR/venv"
sudo -u "$BOT_USER" "$BOT_DIR/venv/bin/pip" install --upgrade pip -q
sudo -u "$BOT_USER" "$BOT_DIR/venv/bin/pip" install -r "$BOT_DIR/requirements-github.txt" -q
info "Зависимости установлены"

info "Установка Playwright (Chromium) для fallback-мониторинга..."
sudo -u "$BOT_USER" "$BOT_DIR/venv/bin/python" -m playwright install chromium
"$BOT_DIR/venv/bin/python" -m playwright install-deps chromium
info "Playwright установлен"

# ── 5. Папка логов ───────────────────────────────────────────────────────────
mkdir -p "$LOG_DIR"
chown "$BOT_USER:$BOT_USER" "$LOG_DIR"
info "Логи: $LOG_DIR"

# ── 6. Файл .env ─────────────────────────────────────────────────────────────
ENV_FILE="$BOT_DIR/.env"
if [ ! -f "$ENV_FILE" ]; then
    cp "$BOT_DIR/deploy/.env.example" "$ENV_FILE"
    chown "$BOT_USER:$BOT_USER" "$ENV_FILE"
    chmod 600 "$ENV_FILE"
    warn "Заполните переменные: sudo nano $ENV_FILE"
else
    info ".env уже существует"
fi

# ── 7. Systemd сервис (демон) ────────────────────────────────────────────────
cp "$BOT_DIR/deploy/basketball-bot.service" /etc/systemd/system/
systemctl daemon-reload
info "Systemd-сервис установлен"

# ── 8. Cron-расписание ───────────────────────────────────────────────────────
cp "$BOT_DIR/deploy/basketball-cron" /etc/cron.d/basketball-bot
chmod 644 /etc/cron.d/basketball-bot
info "Cron-расписание установлено"

# ── 9. Logrotate ─────────────────────────────────────────────────────────────
cat > /etc/logrotate.d/basketball-bot << 'EOF'
/var/log/basketball-bot/*.log {
    daily
    rotate 14
    compress
    missingok
    notifempty
    copytruncate
}
EOF
info "Logrotate настроен"

# ── Итог ─────────────────────────────────────────────────────────────────────
echo ""
echo "============================================="
echo "  Установка завершена!"
echo "============================================="
echo ""
echo "Следующие шаги:"
echo ""
echo "  1. Заполните .env:"
echo "     sudo nano $ENV_FILE"
echo ""
echo "  2. Запустите бота:"
echo "     sudo systemctl enable $SERVICE_NAME"
echo "     sudo systemctl start $SERVICE_NAME"
echo ""
echo "  3. Проверьте статус:"
echo "     sudo systemctl status $SERVICE_NAME"
echo "     sudo journalctl -u $SERVICE_NAME -f"
echo ""
echo "  4. Логи демона:"
echo "     tail -f $LOG_DIR/daemon.log"
echo ""
