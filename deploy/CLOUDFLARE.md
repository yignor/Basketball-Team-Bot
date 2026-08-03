# Именованный туннель Cloudflare для фэнтези-API

Зачем: у части игроков не открывается Tailscale Funnel (`*.ts.net`) — провайдер
режет. Cloudflare с собственным доменом доступен шире. Funnel не убираем: фронт
перебирает адреса по списку, и две независимые двери лучше одной.

Страница Mini App остаётся на GitHub Pages — она открывается у всех, трогать
её незачем. Через Cloudflare идут только данные.

## Один раз на сервере

Всё, кроме шага 2, требует root. Шаг 2 запускается от обычного пользователя:
он открывает браузерную авторизацию в аккаунте Cloudflare.

```bash
# 1. Поставить cloudflared
sudo curl -L https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64 \
     -o /usr/local/bin/cloudflared
sudo chmod +x /usr/local/bin/cloudflared

# 2. Привязать сервер к аккаунту Cloudflare (выведет ссылку — открыть в браузере
#    и выбрать зону one4two.ru). Создаст ~/.cloudflared/cert.pem
cloudflared tunnel login

# 3. Создать туннель и запомнить его UUID
cloudflared tunnel create pullup-api

# 4. Прописать DNS: CNAME api.one4two.ru -> туннель. Запись Cloudflare создаст сам
cloudflared tunnel route dns pullup-api api.one4two.ru

# 5. Разложить конфиг и ключ
sudo mkdir -p /etc/cloudflared
sudo cp /opt/basketball-bot/deploy/cloudflared-config.yml /etc/cloudflared/config.yml
sudo cp ~/.cloudflared/*.json /etc/cloudflared/pullup-api.json
sudo chmod 600 /etc/cloudflared/*.json

# 6. Поднять как сервис
sudo cp /opt/basketball-bot/deploy/cloudflared-api.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now cloudflared-api
```

## Маршрут до Cloudflare — через VPN (обязательно)

На прямом канале провайдер рвёт длинные соединения к edge: туннель
регистрируется, а потом теряет по соединению каждые 2–3 минуты, и снаружи
проходит примерно один запрос из трёх. Лечится одним правилом маршрутизации —
трафик cloudflared (он работает от root) уходит в VPN:

```
ip rule add uidrange 0-0 to 198.41.192.0/20 lookup 51820 priority 39
```

Оно живёт в `/usr/local/sbin/basketball-bot-telegram-route.sh` (копия скрипта —
`deploy/basketball-bot-telegram-route.sh`), иначе исчезнет при перезагрузке.
Результат 03.08.2026: обрывов ноль, 10 ответов из 10, время ответа 0.2с.

Проверить, что правило на месте: `ip rule show | grep 198.41`.

## Если домен отдаёт 502/530

Смотреть надо не на «жив ли сервис», а на **код ошибки в теле ответа**:

```bash
curl -s https://api.one4two.ru/health | grep -o 'error code: [0-9]*'
```

- **1033** — у Cloudflare нет маршрута для этого имени. Коварство в том, что
  туннель при этом выглядит живым: в логе `Registered tunnel connection`,
  соединения переподключаются, UUID в логе совпадает с CNAME в DNS. Смотреть
  надо на страницу туннеля в панели: **Status: Down, Active replicas: 0** при
  живом логе на сервере — это и есть диагноз.

  **Первым делом посмотреть на вкладку Routes страницы туннеля.** Там написано,
  кто хозяин маршрутов, и от этого зависит всё:

  - **«This tunnel is locally managed»** — наш случай. Маршруты берутся из
    `/etc/cloudflared/config.yml`, панель их только показывает и менять не
    даёт. Значит коннектор ОБЯЗАН запускаться с `--config`; без него он
    подключается к edge, но правил маршрутизации у него нет — и домен отдаёт
    1033. **Токен такому туннелю не подходит**: он для тех, что заведены в
    панели. Проверять так:

    ```bash
    ps -eo cmd | grep '[c]loudflared'    # должен быть --config /etc/cloudflared/config.yml
    ```

  - Если бы туннель был заведён в панели, всё наоборот: маршруты настраиваются
    там же, а коннектор подключается токеном (`TUNNEL_TOKEN` в
    `/etc/cloudflared/token`, 600).

  История 03.08.2026: увидели 1033, приняли туннель за панельный, перевели на
  токен — стало только хуже, потому что вместе с токеном коннектор потерял
  локальный ingress. Вернули `--config` — заработало. День потерян на том, что
  не посмотрели на эту надпись сразу.

- **1016 / «no recent network activity»** в логе cloudflared — не проходит QUIC
  (UDP/7844). В конфиге для этого стоит `protocol: http2`: тот же TCP/443, что
  и обычный HTTPS. Проверено 03.08.2026 — с QUIC туннель регистрировался и
  тут же отваливался.

Пока домен лежит, приложение работает: фронт сам перебирает двери и уходит на
Tailscale Funnel. Это не авария, но запасная дверь остаётся одна.

## Проверка

```bash
curl -sS https://api.one4two.ru/health          # ожидаем 200 и короткий ответ
curl -so /dev/null -w '%{http_code}\n' https://api.one4two.ru/fantasy/pool   # 401 — жив, просит подпись
```

401 без подписи — правильный ответ: всё под API требует Telegram initData.

## Что уже сделано в коде

- `docs/fantasy/index.html`: адрес добавлен ПЕРВЫМ в список API_LIST, Funnel
  остался вторым. У кого режется одно — работает другое.
- Порядок перебора не трогать: у Cloudflare шире охват, Funnel — запасной путь.

## Если понадобится снять

```bash
sudo systemctl disable --now cloudflared-api
sudo rm /etc/systemd/system/cloudflared-api.service /etc/cloudflared/config.yml
sudo systemctl daemon-reload
```
