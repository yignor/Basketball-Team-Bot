#!/bin/bash
# Узкая маршрутизация для бота: через VPN (таблица 51820, тот же туннель, что
# настроен в общем ensure-vpn-routes.sh) идёт только то, что без него не
# работает. Всё остальное — напрямую. Общий скрипт/USERS-конфиг не трогаем:
# работаем за счёт более высокого приоритета (меньшее число = раньше).
#
# Кладётся в /usr/local/sbin/basketball-bot-telegram-route.sh (root, 755).
set -u
TABLE=51820
UID_BB="$(id -u botuser 2>/dev/null)" || exit 0

add_rule() {
  local selector="$1" prio="$2" table="$3"
  # Проверяем по НОМЕРУ приоритета, а не по тексту селектора: ядро печатает
  # правило в своём порядке («from all to X uidrange Y-Y lookup Z»), и
  # дословный поиск нашей строки не совпадал никогда. Из-за этого скрипт на
  # каждом запуске пытался добавить существующие правила и ругался
  # «RTNETLINK answers: File exists» — выглядело как поломка, хотя всё стояло.
  if ip rule show | grep -qE "^${prio}:"; then
    return 0
  fi
  ip rule add $selector lookup "$table" priority "$prio" \
    && echo "basketball-bot-telegram-route: added [$selector -> $table, prio $prio]" \
    || echo "basketball-bot-telegram-route: FAILED [$selector -> $table]" >&2
}

# basketstat.su режет иностранные адреса — к нему строго напрямую.
add_rule "to 185.73.215.44" 38 "main"

# Cloudflare edge (туннель api.one4two.ru) — через VPN. На прямом канале
# провайдер рвёт длинные соединения: туннель регистрировался и терял по одному
# соединению каждые 2–3 минуты, снаружи проходил примерно один запрос из трёх.
# С этим правилом — ноль обрывов и 10 ответов из 10 (проверено 03.08.2026).
add_rule "uidrange 0-0 to 198.41.192.0/20" 39 "$TABLE"

# Telegram Bot API у botuser — через VPN, остальное (Google Sheets, лиги) прямо.
add_rule "uidrange ${UID_BB}-${UID_BB} to 149.154.160.0/20" 40 "$TABLE"
add_rule "uidrange ${UID_BB}-${UID_BB} to 91.108.4.0/22"    41 "$TABLE"
add_rule "uidrange ${UID_BB}-${UID_BB}"                     42 "main"
