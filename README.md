# tg-chat-collector

Сборщик сообщений рабочих чатов Телеграма: бот сидит в разрешённых чатах, длинным опросом
забирает поток и складывает его в SQLite. Опционально шлёт каждое сообщение POST-ом
в приложение.

Один файл, только стандартная библиотека, Python 3.11+. Ставить нечего.

```bash
python tg_chat_collector.py                 # длинный опрос, основной режим
python tg_chat_collector.py --once          # один проход — проверка после настройки
python tg_chat_collector.py --status        # смещение, сколько сообщений, какие чаты
python tg_chat_collector.py --list-chats    # все чаты, где бот побывал, с id и статусом
```

## Ограничения Bot API — прочитать до развёртывания

1. **Истории до входа бота нет.** Собирается только поток с момента, когда бота добавили
   в чат. Прошлую переписку Bot API не отдаёт вообще.
2. **Privacy mode.** По умолчанию бот в группе видит только команды, упоминания и ответы
   себе. Нужно @BotFather → `/setprivacy` → **Disable**, и после этого **переподключить
   бота в тех чатах, где он уже состоит** (удалить и добавить заново) — иначе там ничего
   не изменится. Проверка: `getMe` возвращает `can_read_all_group_messages: true`
   (сборщик сам предупредит в stderr, если это не так).
3. **Читатель `getUpdates` у токена ровно один.** Второй процесс или вебхук получает
   HTTP 409, и два читателя воруют апдейты друг у друга. При 409 сборщик не ретраит,
   а выходит с кодом 2: это чинит человек.

   ⚠️ **При переносе на другой хост:** 409 прилетает не тому, кто пришёл вторым, а тому,
   кто уже работает. Не запускайте вторую копию с тем же токеном «просто проверить» —
   уроните работающую. Порядок: остановить старую → запустить новую.
4. **Вложение живёт сутки.** `getFile` не отдаёт файл старше 24 часов, поэтому вложения
   скачиваются в момент приёма сообщения (если задан `MEDIA_DIR`).

## Настройки

Всё через переменные окружения, см. [.env.example](.env.example).

| Переменная | Обязательна | Что это |
|---|---|---|
| `TG_INTAKE_BOT_TOKEN` | да | токен бота от @BotFather. Не должен совпадать с токеном другого бота, который слушает `getUpdates` |
| `ALLOWED_CHATS` | да\* | id разрешённых чатов через запятую |
| `ALLOWED_CHATS_FILE` | да\* | либо путь к json: `{"chats": [{"chat_id": -1001234567890, "title": "…"}]}` |
| `DB_PATH` | нет | файл SQLite, по умолчанию `chats.sqlite3` рядом со скриптом |
| `MEDIA_DIR` | нет | папка для вложений. Не задана — вложения не скачиваются, остаётся только `media_file_id` |
| `INGEST_URL` | нет | HTTP-приёмник приложения: на каждое сообщение уходит POST с JSON |
| `INGEST_TOKEN` | нет | если задан, уходит заголовком `Authorization: Bearer …` |

\* нужен хотя бы один из двух: пустой белый список = ничего не собирается. Это сделано
намеренно: чат, о котором не договорились, в базу не попадает — он лишь показывается
в `--list-chats`, чтобы владелец увидел id и решил.

Первое включение: задать токен без белого списка → `--once` → `--list-chats` → взять оттуда
id нужных чатов → положить их в `ALLOWED_CHATS` → запускать постоянный режим.

## Развёртывание

### systemd

```ini
[Unit]
Description=TG chat collector
After=network-online.target

[Service]
WorkingDirectory=/opt/tg-chat-collector
EnvironmentFile=/etc/tg-chat-collector.env
ExecStart=/usr/bin/python3 /opt/tg-chat-collector/tg_chat_collector.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

`SIGTERM` обрабатывается: процесс дописывает текущую пачку и выходит, смещение остаётся
в базе — перезапуск ничего не теряет и не дублирует.

### Docker

```bash
docker build -t tg-chat-collector .
docker run -d --restart=always \
  -e TG_INTAKE_BOT_TOKEN=… -e ALLOWED_CHATS=-1001234567890 \
  -e DB_PATH=/data/chats.sqlite3 -e MEDIA_DIR=/data/media \
  -v tg-collector-data:/data tg-chat-collector
```

Это **worker**, а не веб-сервис: порт не слушает, HTTP-healthcheck не пройдёт. Под `DB_PATH`
и `MEDIA_DIR` нужен постоянный том — иначе при перезапуске потеряются и архив, и смещение
(сообщения за время простоя Телеграм хранит 24 часа, дальше они пропадают безвозвратно).

Признак живости — поле `updated_at` в таблице `state` двигается каждые ~25 секунд.
**Проверять живость чужим `getUpdates` нельзя** — см. пункт 3 ограничений.

## Схема базы

- `chats` — `chat_id`, `title`, `username`, `kind`, `seen_at_utc`, `allowed`
- `senders` — `sender_id`, `name`, `username`
- `messages` — `chat_id` + `message_id` (первичный ключ), `sender_id`, `date_utc`, `text`,
  `reply_to_message_id`, `topic_id`, `forwarded`, `media_type`, `media_file_id`,
  `media_path`, `media_status`, `edited_at_utc`, `service_type`
- `state` — `offset`, `updated_at`, `stats`

Запись идемпотентна по `(chat_id, message_id)`: повторный разбор пачки не плодит дублей,
правка сообщения обновляет текст на месте.

```sql
SELECT c.title, s.name, m.date_utc, m.text
FROM messages m
JOIN chats c ON c.chat_id = m.chat_id
LEFT JOIN senders s ON s.sender_id = m.sender_id
WHERE m.date_utc >= datetime('now', '-1 day')
ORDER BY m.date_utc;
```

## Формат пересылки (`INGEST_URL`)

```json
{
  "chat": {"id": -1001234567890, "title": "Название чата", "type": "supergroup"},
  "message": {
    "chat_id": -1001234567890, "message_id": 12345, "sender_id": 111,
    "date_utc": "2026-08-28T05:12:07+00:00", "text": "…",
    "reply_to_message_id": null, "topic_id": null, "forwarded": 0,
    "media_type": "voice", "media_file_id": "AwAC…", "media_path": "/data/media/…/12345.ogg",
    "media_status": "ok", "edited_at_utc": null, "service_type": null
  }
}
```

Приёмник должен отвечать 2xx. Ошибка пересылки печатается в stderr и **не** роняет сбор:
источник правды — база, недостающее догоняется по ней.

## Данные и приватность

В базе лежит сырая переписка. Держите её в приватном контуре, не коммитьте базу и медиа
(`.gitignore` уже это делает), токен бота — только в переменных окружения.

Сборщик ничего не отправляет, не редактирует и не удаляет в Телеграме: используются только
`getUpdates`, `getMe` и `getFile`.

## Лицензия

MIT, см. [LICENSE](LICENSE).
