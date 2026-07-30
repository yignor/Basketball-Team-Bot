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
