#!/bin/bash
# Поднимает Cloudflare quick-tunnel к локальному фэнтези-API и записывает
# публичный HTTPS-URL в файл, который читает бот (кнопка «Открыть фэнтези»).
# Quick-tunnel бесплатен и не требует домена; URL случайный и меняется при
# каждом старте — поэтому пишем его в файл, а бот подставляет актуальный.
#
# Требуется установленный cloudflared:
#   curl -L https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64 -o /usr/local/bin/cloudflared
#   chmod +x /usr/local/bin/cloudflared
set -euo pipefail

PORT="${FANTASY_API_PORT:-8081}"
URL_FILE="${FANTASY_API_URL_FILE:-/opt/basketball-bot/data/fantasy_api_url.txt}"

# Гарантируем каталог для файла URL
mkdir -p "$(dirname "$URL_FILE")"

echo "cloudflared-fantasy: туннель к http://127.0.0.1:${PORT}, URL -> ${URL_FILE}"

# --protocol http2: держим связь с Cloudflare по TCP 443, а не QUIC/UDP —
# через VPN-туннель UDP/QUIC рвётся («origin unregistered from Argo Tunnel»).
cloudflared tunnel --no-autoupdate --protocol http2 --url "http://127.0.0.1:${PORT}" 2>&1 | while IFS= read -r line; do
  echo "$line"
  if [[ "$line" =~ (https://[a-z0-9-]+\.trycloudflare\.com) ]]; then
    echo "${BASH_REMATCH[1]}" > "$URL_FILE"
    echo "cloudflared-fantasy: записал URL ${BASH_REMATCH[1]}"
  fi
done
