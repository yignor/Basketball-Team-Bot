#!/bin/bash
# Проверка бота после перезагрузки сервера (или когда «что-то не так»).
#
# Запускается ОТ ignor, пароля не просит: все sudo-строки совпадают с теми,
# что разрешены в sudoers. Ничего не чинит — только говорит, что живо, а что
# нет, и что с этим делать.
#
# Копия в репозитории: deploy/healthcheck.sh, на сервере удобно держать её же
# по этому пути и звать `bash /opt/basketball-bot/deploy/healthcheck.sh`.

ok()   { printf '  ✅ %s\n' "$1"; }
bad()  { printf '  ❌ %s\n' "$1"; FAILED=1; }
warn() { printf '  ⚠️  %s\n' "$1"; }

FAILED=0
echo "▶️  Бот"
# Без sudo: is-active и show прав не требуют, а разрешённой строки sudoers
# для них нет — с sudo проверка молча падала бы в «служба не работает».
if systemctl is-active basketball-bot >/dev/null 2>&1; then
  since=$(systemctl show basketball-bot -p ActiveEnterTimestamp --value 2>/dev/null)
  ok "служба работает (с $since)"
else
  bad "служба НЕ работает → sudo systemctl start basketball-bot"
fi

# Главная ловушка после перезагрузки: бот отвечает в Telegram, а API молчит —
# значит Mini App у всех мёртв, и заметить это по журналу непросто.
echo "▶️  Фэнтези-API (Mini App)"
if ss -ltn 2>/dev/null | grep -q '127.0.0.1:8081'; then
  code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 http://127.0.0.1:8081/fantasy/pool)
  # 401 — это ЗДОРОВЫЙ ответ: без подписи Telegram внутрь не пускают.
  if [ "$code" = "401" ] || [ "$code" = "200" ]; then
    ok "слушает порт 8081, отвечает ($code)"
  else
    bad "порт открыт, но ответ $code → sudo systemctl restart basketball-bot"
  fi
else
  bad "порт 8081 не слушает → sudo systemctl restart basketball-bot"
fi

echo "▶️  Двери снаружи"
if systemctl is-active cloudflared-api >/dev/null 2>&1; then
  ok "Cloudflare-туннель (основная дверь) активен"
else
  bad "Cloudflare-туннель не активен → sudo systemctl start cloudflared-api"
fi
if tailscale status >/dev/null 2>&1; then
  ok "Tailscale поднят (запасная дверь)"
else
  warn "Tailscale не отвечает — основная дверь Cloudflare, это не срочно"
fi

echo "▶️  Маршрутизация"
# 38 — basketstat напрямую (лига режет иностранные IP), 39 — Cloudflare через
# VPN (иначе туннель рвётся), 40-42 — Telegram у botuser через VPN.
missing=""
for prio in 38 39 40 41 42; do
  ip rule show | grep -qE "^${prio}:" || missing="$missing $prio"
done
if [ -z "$missing" ]; then
  ok "правила 38–42 на месте"
else
  bad "нет правил:$missing → sudo /usr/local/sbin/basketball-bot-telegram-route.sh"
fi

echo "▶️  Расписание"
if [ -f /etc/cron.d/basketball-bot ]; then
  ok "крон на месте ($(grep -cE '^[0-9*]' /etc/cron.d/basketball-bot) заданий)"
else
  bad "нет /etc/cron.d/basketball-bot → sudo cp deploy/basketball-cron /etc/cron.d/basketball-bot"
fi

echo "▶️  Данные"
db=/opt/basketball-bot/data/bot.db
if [ -r "$db" ]; then
  cp "$db" /tmp/hc.db 2>/dev/null
  cp "$db-wal" /tmp/hc.db-wal 2>/dev/null
  games=$(python3 -c "import sqlite3;print(sqlite3.connect('/tmp/hc.db').execute('SELECT COUNT(*) FROM game_player_stats').fetchone()[0])" 2>/dev/null)
  rm -f /tmp/hc.db /tmp/hc.db-wal
  [ -n "$games" ] && ok "база читается, строк статистики: $games" || bad "база не читается"
else
  bad "нет файла базы $db"
fi

echo
if [ "$FAILED" = "1" ]; then
  echo "Итог: есть проблемы — см. ❌ выше."
  exit 1
fi
echo "Итог: всё на месте, вмешательства не требуется."
