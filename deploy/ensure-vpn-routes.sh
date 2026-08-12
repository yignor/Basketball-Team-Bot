#!/bin/bash
# Ensures each bot's system user routes its traffic through the awg0 VPN
# tunnel (table 51820), since Telegram is blocked directly by the ISP.
# Idempotent: safe to run repeatedly (boot + periodic timer) in case the
# kernel policy rule gets lost (observed once after initial setup).
#
# Exception: golfbot's parser targets some .ru golf-club sites (and
# pestovo.golf) that are only reachable from a direct Russian IP and break
# over the VPN exit node. Those specific hosts are routed directly
# (bypassing the tunnel) via higher-priority per-destination rules.
#
# Обслуживает ОБА бота: баскетбольного (botuser) и гольфа (golfbot). Ставится
# в /usr/local/sbin, запускается кроном /etc/cron.d/bot-vpn-route раз в две
# минуты. Здесь лежит версия под контролем версий — правь её, а не серверную.
#
# Жалобы на неразрешимое имя — не чаще раза в сутки (12.08.2026). До этого
# лог рос на 720 одинаковых строк в день: pestovo.golf почти никогда не
# резолвится (28663 неудачи против 4 удач), и за месяц он занял 1,7 МБ, в
# которых полезного не было почти ничего. Убрать хост из списка нельзя —
# парсер гольф-бота ходит именно туда, и в те редкие разы, когда имя всё же
# разрешается, прямой маршрут ему нужен.
set -u
TABLE=51820
STATE_DIR=/var/lib/ensure-vpn-routes

# Про неразрешимое имя говорим раз в сутки на хост. Отметка — файл с датой
# последней жалобы; при удачном разрешении она стирается, чтобы следующая
# поломка не молчала сутки.
complain_once_a_day() {
  local host="$1"
  local stamp="${STATE_DIR}/unresolved-${host//[^A-Za-z0-9._-]/_}"
  local today
  today="$(date +%F)"
  [ "$(cat "$stamp" 2>/dev/null)" = "$today" ] && return 0
  mkdir -p "$STATE_DIR" 2>/dev/null
  echo "$today" > "$stamp" 2>/dev/null
  echo "ensure-vpn-routes: $host не резолвится (повторы за сутки не пишу)" >&2
}

forget_complaint() {
  local host="$1"
  rm -f "${STATE_DIR}/unresolved-${host//[^A-Za-z0-9._-]/_}" 2>/dev/null
}

declare -A USERS=( [botuser]=100 [golfbot]=101 )
for user in "${!USERS[@]}"; do
  prio="${USERS[$user]}"
  uid="$(id -u "$user" 2>/dev/null)" || { echo "ensure-vpn-routes: user $user not found" >&2; continue; }
  if ip rule show | grep -q "uidrange ${uid}-${uid} lookup ${TABLE}"; then
    :
  else
    if ip rule add uidrange "${uid}-${uid}" lookup "${TABLE}" priority "$prio"; then
      echo "ensure-vpn-routes: added tunnel rule for $user (uid $uid, priority $prio)"
    else
      echo "ensure-vpn-routes: FAILED to add tunnel rule for $user (uid $uid)" >&2
    fi
  fi
done

GOLF_DIRECT_HOSTS=(gorkigolf.ru peterhofgolf.ru sf-golfclub.ru millcreek.ru forestgolf.ru pestovo.golf)
golfbot_uid="$(id -u golfbot 2>/dev/null)" || golfbot_uid=""
if [ -n "$golfbot_uid" ]; then
  for host in "${GOLF_DIRECT_HOSTS[@]}"; do
    ip="$(getent ahostsv4 "$host" 2>/dev/null | awk '{print $1}' | head -1)"
    if [ -z "$ip" ]; then
      complain_once_a_day "$host"
      continue
    fi
    forget_complaint "$host"
    if ip rule show | grep -q "to ${ip} uidrange ${golfbot_uid}-${golfbot_uid} lookup main"; then
      :
    else
      if ip rule add uidrange "${golfbot_uid}-${golfbot_uid}" to "${ip}/32" lookup main priority 50; then
        echo "ensure-vpn-routes: added direct-bypass rule for golfbot -> $host ($ip)"
      else
        echo "ensure-vpn-routes: FAILED to add direct-bypass rule for $host ($ip)" >&2
      fi
    fi
  done
fi
