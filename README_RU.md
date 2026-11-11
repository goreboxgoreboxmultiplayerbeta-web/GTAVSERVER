# Grand-V Relay: Render + GitHub

Самостоятельный WebSocket-ретранслятор **официального протокола Grand-V версии 1**. Полноценный игровой сервер GTA 5 из этого не получится: игровой клиент Grand-V должен сам отправлять данные и рисовать чужих игроков. Оригинальный протокол НЕ передаёт состояние машин, анимации и физику.

## Загрузка на GitHub

1. Распакуйте ZIP. В корень нового репозитория загрузите `server.py`, `requirements.txt`, `render.yaml` и `README_RU.md`.
2. Создайте на Render **New > Web Service**, выберите репозиторий.
3. Runtime: Python; Build Command: `pip install -r requirements.txt`; Start Command: `python server.py`.
4. Environment Variable `RELAY_ACCESS_CODE`: придумайте собственный секретный код, запомните его. Не добавляйте код в GitHub.
5. Deploy. Проверка: `https://ВАШ_СЕРВЕР.onrender.com/health` отдаёт `status: ok`.

## В лаунчере GTA

- Multiplayer: ON
- Use this website's relay: OFF
- Relay address: `wss://ВАШ_СЕРВЕР.onrender.com/relay`
- Room: `DanilaServer` (любой латинский идентификатор до 32 символов)
- Access code: совпадает с `RELAY_ACCESS_CODE` на Render (с учётом регистра)
- У второго игрока должны быть те же комната/код и совместимая сборка клиента/карта.

## Диагностика

Откройте `/stats` на сервере. Когда два игрока вошли, `players: 2`. Когда персонаж одного двигается, `poses_received` растёт. Если `poses_forwarded` тоже растёт — сервер передаёт положения второму игроку. Если во второй игре игрок не появляется — это уже клиентская проблема, а не проблема веб-сокет-транспорта.

`check`, `join`, `welcome`, `joined`, `pose`, `left` реализованы по публичному оригинальному коду `LauncherApp/rust/src/relay.rs`: https://github.com/SebasKitten/Grand-V . В одной комнате не более двух человек. При рестарте сервера комнаты исчезают.

## Локальная проверка

```bash
pip install -r requirements.txt
pip install pytest
python -m pytest -q tests
RELAY_ACCESS_CODE=YOURSECRET python server.py  # Linux/macOS
# PowerShell: $env:RELAY_ACCESS_CODE='YOURSECRET'; python server.py
```

Этот проект не содержит игровой контент Rockstar, игровые ассеты или изменённый клиент. Он только ретранслирует данные, которые уже умеет передавать Grand-V. Чтобы синхронизировать машины и анимации, необходимы изменения клиента и новый согласованный протокол.

На Render Free сервер засыпает после 15 минут без входящих запросов и WebSocket-сообщений; просыпание может занять время.
