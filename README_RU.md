# Grand-V Multiplayer Relay — Render + GitHub

Небольшой сервер для экспериментального мультиплеера Grand-V (GTA в браузере). Реализует **protocol 1** из оригинального `LauncherApp/rust/src/relay.rs`.

**Это только ретранслятор позиций двух игроков.** Проект не включает файлы GTA V, не запускает саму игру на Render и не добавляет синхронизацию транспорта, стрельбы или заданий.

## 1. Загрузить в GitHub (можно с телефона)
1. Распакуйте ZIP-файл.
2. На GitHub откройте **New repository**, назовите `grandv-render-relay`, нажмите **Create repository**.
3. Нажмите **Add file → Upload files** (на смартфоне в Chrome при необходимости включите **Версия для ПК**).
4. Загрузите `server.py`, `requirements.txt`, `render.yaml`, `.gitignore`, `README_RU.md` и папку `tests` (если папку неудобно загружать, тесты необязательны).
5. Нажмите **Commit changes**.

## 2. Render
1. Откройте https://dashboard.render.com/ → **New → Web Service**.
2. Подключите GitHub и выберите `grandv-render-relay`.
3. Укажите **Language / Runtime: Python 3** и **Instance type: Free**.
4. Build Command: `pip install -r requirements.txt`.
5. Start Command: `python server.py`.
6. В **Environment Variables** добавьте `RELAY_ACCESS_CODE` и ваш секретный код (пример: `danilaGame2026`). Этот код потребуется всем участникам при подключении к ВАШЕМУ серверу. Регистр букв важен. Не используйте пример как пароль публичного сервера.
7. Нажмите **Deploy Web Service**.
8. Откройте `https://ИМЯ-СЕРВИСА.onrender.com/health`. Если написано `"status":"ok"`, сервер работает.

**Альтернатива:** можно открыть Render → **New → Blueprint** и выбрать репозиторий с `render.yaml`. В процессе создания Render должен запросить `RELAY_ACCESS_CODE`.

## 3. Подключение GTA в браузере
1. Откройте https://gta.zip.wf/launcher.html.
2. Разверните **Multiplayer settings**.
3. Включите **Enable multiplayer for this launch**.
4. **ВЫКЛЮЧИТЕ** `Use this website's relay`, иначе будет использоваться сервер сайта, а не ваш.
5. В `Relay address` напишите `wss://ИМЯ-СЕРВИСА.onrender.com/relay` (именно `wss://`, с путём `/relay`).
6. В `Room name` укажите, например, `DanilaServer` (только латинские буквы, цифры, `_` или `-`, длина до 32).
7. В `Access code` введите **точно** значение `RELAY_ACCESS_CODE` из Render. Это не отдельный пароль каждой комнаты, а общий секрет всего ретранслятора.
8. Запускайте **Sandbox** (режим тестового мультиплеера).
9. Друг открывает ту же игру, задаёт **тот же WSS-адрес, комнату и код**. При наличии поддержки движка вы увидите друг друга.

## 4. Проверка и проблемы
- В браузере по адресу `https://ИМЯ-СЕРВИСА.onrender.com/health` должен быть JSON со `status: ok`.
- `rooms` и `players` увеличиваются по мере подключений.
- Не подключается: проверьте `wss://`, окончание `/relay`, код и одинаковую сборку игры и карту у обоих участников.
- `Join key rejected`: неверный `Access code`.
- `Room build/map mismatch`: участники используют разные сборки или карты.
- `Room full`: в комнате уже 2 игрока.
- Через браузерную строку `wss://.../relay` не открывается обычная страница — это нормально, маршрут предназначен для WebSocket.
- Ошибку WebSocket и коды закрытия можно искать в DevTools на ПК; на Render — **Logs**.
- Если сайт не позволяет адреса сторонних ретрансляторов или его клиент изменился, отдельный Render-сервер этого не исправит. Протокол сервера совпадает с опубликованным кодом Grand-V, но реальная совместимость с текущей сборкой сайта требует проверки в игре.

## 5. Бесплатный Render
Бесплатная служба засыпает после 15 минут без HTTP-запросов и входящих WebSocket-сообщений. Первая загрузка после сна может занять около минуты. Комнаты исчезают при перезапуске, и игрокам потребуется подключаться заново. Постоянный режим без спящего сервера требует платного плана или другой инфраструктуры.

## Локальные тесты (необязательно)
```bash
pip install -r requirements.txt
python -m unittest discover -s tests -v
```

Исходный протокол: https://github.com/SebasKitten/Grand-V/blob/main/LauncherApp/rust/src/relay.rs

Render WebSockets: https://render.com/docs/websocket
