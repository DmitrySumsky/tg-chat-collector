"""TG CHAT COLLECTOR — сборщик сообщений рабочих чатов Телеграма.

v1.0.1 — 07.10.2026

Один файл, только стандартная библиотека, своя база SQLite. Бот сидит в разрешённых чатах,
длинным опросом забирает поток сообщений и складывает его туда, откуда читает приложение:
в SQLite и/или в HTTP-приёмник (`INGEST_URL`).

Зачем: рабочая переписка — источник контекста для сервисов и агентов, но выгрузить её
из Телеграма постфактум нельзя. Сбор ведётся с момента подключения бота и дальше.

ИСТОРИЯ ВЕРСИЙ

v1.0.1 — 07.10.2026
СБОРЩИК ПАДАЛ НА ОБРЫВЕ СВЯЗИ — первый боевой запуск под launchd, три падения за вечер.
• `ConnectionResetError`, таймаут и прочие сетевые ошибки на getUpdates больше не роняют
  процесс: пишется строка в stderr, пауза с нарастанием (5 → 10 → … → 60 с), опрос с того же
  смещения. Ни одно сообщение не теряется: очередь подтверждается только следующим запросом.
• HTTP 5xx Телеграма — так же, с паузой. HTTP 409 (второй читатель) по-прежнему останавливает сбор.

v1.0.0 — 28.08.2026
СБОРЩИК ДОЛЖЕН ЖИТЬ РЯДОМ С ПРИЛОЖЕНИЕМ, А НЕ НА МАШИНЕ АВТОРА — первая публичная версия.
• Один файл вместо пакета: разворачивается копированием, править может кто угодно.
• Только стандартная библиотека: ставить нечего, запускается любым python 3.11+.
• Схема SQLite `chats` / `messages` / `senders` / `state`: приложение читает базу
  напрямую, отдельного формата обмена не требуется.
• Смещение очереди хранится в базе, а не в json рядом с кодом: контейнер переживает
  перезапуск, файловая система может быть эфемерной.
• Необязательная пересылка каждого сообщения POST-ом (`INGEST_URL`) — приложение получает
  поток сразу, не читая чужую базу.
• Голосовые не расшифровываются: скачивается сам файл (`MEDIA_DIR`), расшифровка — задача
  приложения (у него уже есть модель, тащить вторую на сервер сбора незачем).

ОГРАНИЧЕНИЯ BOT API, из которых растёт вся конструкция:

* историю ДО входа бота получить нельзя — собирается только поток с момента подключения;
* при включённом privacy mode в группе бот видит лишь команды, упоминания и ответы себе.
  Нужен @BotFather → /setprivacy → Disable, после чего бота ПЕРЕПОДКЛЮЧИТЬ в чатах,
  где он уже состоит (удалить и добавить заново), иначе там ничего не изменится;
* читатель `getUpdates` у токена может быть только ОДИН. Второй поллер или вебхук получают
  HTTP 409 и воруют апдейты друг у друга. При 409 процесс не ретраит, а останавливается:
  лечит это человек, а не повтор запроса;
* файл, которому больше суток, `getFile` уже не отдаёт — вложение скачивается сразу.

ЗАПУСК

    python tg_chat_collector.py                 # длинный опрос, пока не остановят
    python tg_chat_collector.py --once          # один проход, для проверки
    python tg_chat_collector.py --status        # состояние без обращения к сети
    python tg_chat_collector.py --list-chats    # чаты, которые бот видел, и их id

Настройки — переменные окружения, см. README.md.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

VERSION = "1.0.0"
API = "https://api.telegram.org/bot{token}/{method}"
UPDATE_KINDS = ("message", "edited_message", "channel_post", "edited_channel_post")
MEDIA_KINDS = ("voice", "video_note", "audio", "photo", "video", "document", "sticker")


class Stop(RuntimeError):
    """Остановка, которую нельзя лечить ретраем: нужен человек."""


# --- настройки ----------------------------------------------------------------------


def setting(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def token() -> str:
    value = setting("TG_INTAKE_BOT_TOKEN")
    if not value:
        raise Stop(
            "нет TG_INTAKE_BOT_TOKEN. Это токен бота, который сидит в рабочих чатах.\n"
            "У одного токена может быть только один читатель getUpdates — не используйте\n"
            "здесь токен бота, который уже слушает обновления где-то ещё."
        )
    return value


def db_path() -> Path:
    return Path(setting("DB_PATH", "chats.sqlite3")).expanduser()


def media_root() -> Path | None:
    value = setting("MEDIA_DIR")
    return Path(value).expanduser() if value else None


def allowed_chats(connection: sqlite3.Connection) -> dict[int, str]:
    """Белый список: id через запятую в ALLOWED_CHATS, либо файл ALLOWED_CHATS_FILE.

    Пустой список означает «не собирать ничего»: чат, о котором не договорились,
    в базу не попадает, а только показывается в `--list-chats`.
    """
    raw = setting("ALLOWED_CHATS")
    ids: list[int] = []
    if raw:
        ids += [int(part) for part in raw.replace(";", ",").split(",") if part.strip()]
    path = setting("ALLOWED_CHATS_FILE")
    if path and Path(path).exists():
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        ids += [int(item["chat_id"]) for item in payload.get("chats", [])]
    titles = dict(connection.execute("SELECT chat_id, COALESCE(title, '') FROM chats").fetchall())
    return {chat_id: titles.get(chat_id, "") for chat_id in ids}


# --- база -------------------------------------------------------------------------


SCHEMA = """
CREATE TABLE IF NOT EXISTS chats (
    chat_id     INTEGER PRIMARY KEY,
    title       TEXT,
    username    TEXT,
    kind        TEXT,
    seen_at_utc TEXT NOT NULL,
    allowed     INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS senders (
    sender_id      INTEGER PRIMARY KEY,
    name           TEXT,
    username       TEXT,
    updated_at_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS messages (
    chat_id             INTEGER NOT NULL,
    message_id          INTEGER NOT NULL,
    sender_id           INTEGER,
    date_utc            TEXT NOT NULL,
    received_at_utc     TEXT NOT NULL,
    text                TEXT,
    reply_to_message_id INTEGER,
    topic_id            INTEGER,
    forwarded           INTEGER NOT NULL DEFAULT 0,
    media_type          TEXT,
    media_file_id       TEXT,
    media_path          TEXT,
    media_status        TEXT,
    edited_at_utc       TEXT,
    service_type        TEXT,
    PRIMARY KEY (chat_id, message_id)
);

CREATE INDEX IF NOT EXISTS messages_by_date ON messages(date_utc);
CREATE INDEX IF NOT EXISTS messages_by_chat_date ON messages(chat_id, date_utc);

CREATE TABLE IF NOT EXISTS state (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


def connect() -> sqlite3.Connection:
    path = db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.executescript(SCHEMA)
    connection.commit()
    return connection


def get_state(connection: sqlite3.Connection, key: str, default: str = "") -> str:
    row = connection.execute("SELECT value FROM state WHERE key = ?", (key,)).fetchone()
    return row[0] if row else default


def set_state(connection: sqlite3.Connection, key: str, value: str) -> None:
    connection.execute(
        "INSERT INTO state(key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, value),
    )


def now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


# --- Telegram Bot API ---------------------------------------------------------------


def call(bot_token: str, method: str, payload: dict, timeout: int = 60):
    data = urllib.parse.urlencode(
        {key: value for key, value in payload.items() if value is not None}
    ).encode()
    request = urllib.request.Request(API.format(token=bot_token, method=method), data=data)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        detail = error.read().decode("utf-8", "replace")
        message = f"{method}: HTTP {error.code} {detail}"
        # 409 — второй читатель очереди. Ретрай тут вреден: два процесса будут отбирать
        # апдейты друг у друга, поэтому это отдельный тип ошибки, останавливающий сбор.
        raise (Stop(message) if error.code == 409 else RuntimeError(message)) from error
    if not body.get("ok"):
        raise RuntimeError(f"{method}: {body}")
    return body["result"]


def download(bot_token: str, file_id: str, destination: Path) -> None:
    info = call(bot_token, "getFile", {"file_id": file_id}, timeout=30)
    url = f"https://api.telegram.org/file/bot{bot_token}/{info['file_path']}"
    destination.parent.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(url, timeout=120) as response, destination.open("wb") as target:
        target.write(response.read())


# --- разбор апдейта -----------------------------------------------------------------


def media_of(message: dict) -> tuple[str | None, str | None]:
    """Тип и file_id вложения. У фото берём самый крупный размер — он последний в списке."""
    for kind in MEDIA_KINDS:
        item = message.get(kind)
        if not item:
            continue
        if kind == "photo":
            return kind, item[-1]["file_id"]
        return kind, item.get("file_id")
    return None, None


def service_of(message: dict) -> str | None:
    for key in ("new_chat_members", "left_chat_member", "pinned_message", "new_chat_title"):
        if key in message:
            return key
    return None


def row_of(message: dict, sender_id: int | None) -> dict:
    media_type, media_id = media_of(message)
    reply = message.get("reply_to_message") or {}
    edit_date = message.get("edit_date")
    return {
        "chat_id": int(message["chat"]["id"]),
        "message_id": int(message["message_id"]),
        "sender_id": sender_id,
        "date_utc": datetime.fromtimestamp(int(message["date"]), UTC).isoformat(timespec="seconds"),
        "received_at_utc": now(),
        "text": message.get("text") or message.get("caption") or "",
        "reply_to_message_id": int(reply["message_id"]) if reply.get("message_id") else None,
        "topic_id": message.get("message_thread_id"),
        "forwarded": 1 if (message.get("forward_origin") or message.get("forward_date")) else 0,
        "media_type": media_type,
        "media_file_id": media_id,
        "media_path": None,
        "media_status": None,
        "edited_at_utc": (
            datetime.fromtimestamp(int(edit_date), UTC).isoformat(timespec="seconds")
            if edit_date
            else None
        ),
        "service_type": service_of(message),
    }


# --- приёмник приложения -----------------------------------------------------------------


def forward(row: dict, chat: dict) -> None:
    """Необязательная пересылка сообщения в приложение. Сбой пересылки не роняет сбор:
    база остаётся источником правды, повторную выгрузку можно сделать по ней."""
    url = setting("INGEST_URL")
    if not url:
        return
    payload = json.dumps({"chat": chat, "message": row}, ensure_ascii=False).encode()
    request = urllib.request.Request(url, data=payload, method="POST")
    request.add_header("Content-Type", "application/json; charset=utf-8")
    ingest_token = setting("INGEST_TOKEN")
    if ingest_token:
        request.add_header("Authorization", f"Bearer {ingest_token}")
    try:
        with urllib.request.urlopen(request, timeout=20):
            pass
    except Exception as error:  # noqa: BLE001 — любая сетевая беда не должна ронять опрос
        print(f"пересылка в {url} не удалась: {error}", file=sys.stderr)


# --- сбор ---------------------------------------------------------------------------


class Collector:
    def __init__(self, bot_token: str, connection: sqlite3.Connection):
        self.token = bot_token
        self.db = connection
        self.allowed = allowed_chats(connection)
        self.media_root = media_root()
        self.stats = {"saved": 0, "skipped": 0, "media": 0}

    def remember_chat(self, chat: dict) -> None:
        chat_id = int(chat["id"])
        self.db.execute(
            """
            INSERT INTO chats(chat_id, title, username, kind, seen_at_utc, allowed)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                title = excluded.title,
                username = excluded.username,
                kind = excluded.kind,
                seen_at_utc = excluded.seen_at_utc,
                allowed = excluded.allowed
            """,
            (
                chat_id,
                chat.get("title") or chat.get("username"),
                chat.get("username"),
                chat.get("type"),
                now(),
                1 if chat_id in self.allowed else 0,
            ),
        )

    def remember_sender(self, user: dict | None) -> int | None:
        if not user:
            return None
        sender_id = int(user["id"])
        name = " ".join(part for part in (user.get("first_name"), user.get("last_name")) if part)
        self.db.execute(
            """
            INSERT INTO senders(sender_id, name, username, updated_at_utc)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(sender_id) DO UPDATE SET
                name = excluded.name,
                username = excluded.username,
                updated_at_utc = excluded.updated_at_utc
            """,
            (sender_id, name or None, user.get("username"), now()),
        )
        return sender_id

    def handle(self, update: dict) -> None:
        for kind in UPDATE_KINDS:
            message = update.get(kind)
            if message:
                break
        else:
            return

        chat = message["chat"]
        self.remember_chat(chat)
        if int(chat["id"]) not in self.allowed:
            # Чат, о котором не договорились, в архив не пишем — только показываем в --list-chats.
            self.stats["skipped"] += 1
            return

        row = row_of(message, self.remember_sender(message.get("from")))
        if row["media_type"] and row["media_file_id"] and self.media_root:
            self.fetch_media(row)
        self.save(row)
        forward(row, chat)
        self.stats["saved"] += 1

    def fetch_media(self, row: dict) -> None:
        suffix = {"voice": ".ogg", "video_note": ".mp4", "photo": ".jpg"}.get(row["media_type"], "")
        destination = self.media_root / str(row["chat_id"]) / f"{row['message_id']}{suffix}"
        try:
            download(self.token, row["media_file_id"], destination)
        except Exception as error:  # noqa: BLE001 — файл старше суток Bot API уже не отдаёт
            row["media_status"] = str(error)[:200]
            return
        row["media_path"] = str(destination)
        row["media_status"] = "ok"
        self.stats["media"] += 1

    def save(self, row: dict) -> None:
        """Запись идемпотентна по (chat_id, message_id): повторный разбор пачки после
        падения не плодит дублей, а правка сообщения обновляет текст на месте."""
        self.db.execute(
            """
            INSERT INTO messages(
                chat_id, message_id, sender_id, date_utc, received_at_utc, text,
                reply_to_message_id, topic_id, forwarded, media_type, media_file_id,
                media_path, media_status, edited_at_utc, service_type)
            VALUES (:chat_id, :message_id, :sender_id, :date_utc, :received_at_utc, :text,
                :reply_to_message_id, :topic_id, :forwarded, :media_type, :media_file_id,
                :media_path, :media_status, :edited_at_utc, :service_type)
            ON CONFLICT(chat_id, message_id) DO UPDATE SET
                text = excluded.text,
                edited_at_utc = excluded.edited_at_utc,
                media_path = COALESCE(excluded.media_path, messages.media_path),
                media_status = COALESCE(excluded.media_status, messages.media_status)
            """,
            row,
        )


def poll_once(collector: Collector, offset: int, wait: int) -> int:
    """Возвращает новое смещение. Очередь подтверждается СЛЕДУЮЩИМ запросом, то есть уже
    после того, как пачка разобрана и записана: падение в середине не теряет сообщения."""
    try:
        batch = call(
            collector.token,
            "getUpdates",
            {
                "offset": offset,
                "timeout": wait,
                "allowed_updates": json.dumps(list(UPDATE_KINDS)),
            },
            timeout=wait + 20,
        )
    except Stop as error:
        raise Stop(
            "HTTP 409: у этого токена уже есть читатель getUpdates или установлен вебхук.\n"
            "Читатель может быть только один — остановите второй процесс или снимите вебхук\n"
            f"(deleteWebhook). Ответ Телеграма: {error}"
        ) from error
    except (OSError, RuntimeError) as error:  # v1.0.1: обрыв, таймаут, 5xx — переждать
        collector.failures = getattr(collector, "failures", 0) + 1
        pause = min(60, 5 * 2 ** (collector.failures - 1))
        print(f"{now()} getUpdates: {type(error).__name__}: {error}; повтор через {pause} с",
              file=sys.stderr, flush=True)
        time.sleep(pause)
        return offset
    collector.failures = 0

    for update in batch:
        collector.handle(update)
        offset = int(update["update_id"]) + 1
    return offset


# --- команды ------------------------------------------------------------------------


def run(*, once: bool, wait: int) -> int:
    bot_token = token()
    connection = connect()
    collector = Collector(bot_token, connection)

    identity = call(bot_token, "getMe", {}, timeout=30)
    if not identity.get("can_read_all_group_messages"):
        print(
            f"ВНИМАНИЕ: у @{identity.get('username')} включён privacy mode — в группах он видит\n"
            "только команды, упоминания и ответы себе. Лечение: @BotFather → /setprivacy →\n"
            "Disable, затем переподключить бота в чатах, где он уже состоит.",
            file=sys.stderr,
        )
    if not collector.allowed:
        print(
            "белый список пуст (ALLOWED_CHATS / ALLOWED_CHATS_FILE) — в базу ничего не пишется.\n"
            "Запустите с --once, затем --list-chats: там будут id всех чатов, где сидит бот.",
            file=sys.stderr,
        )

    stopping = {"now": False}

    def on_signal(signum, frame):  # noqa: ARG001
        stopping["now"] = True

    for name in ("SIGTERM", "SIGINT"):
        if hasattr(signal, name):
            signal.signal(getattr(signal, name), on_signal)

    offset = int(get_state(connection, "offset", "0") or 0)
    while True:
        offset = poll_once(collector, offset, wait)
        set_state(connection, "offset", str(offset))
        set_state(connection, "updated_at", now())
        set_state(connection, "stats", json.dumps(collector.stats, ensure_ascii=False))
        connection.commit()
        if once or stopping["now"]:
            break
        time.sleep(1)

    print(
        f"сохранено: {collector.stats['saved']}, "
        f"пропущено (чат не разрешён): {collector.stats['skipped']}, "
        f"скачано вложений: {collector.stats['media']}"
    )
    return 0


def status() -> int:
    connection = connect()
    allowed = allowed_chats(connection)
    total = connection.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
    print(f"версия: {VERSION}   база: {db_path()}")
    print(f"смещение: {get_state(connection, 'offset', '0')}   обновлено: {get_state(connection, 'updated_at', '—')}")
    print(f"сообщений в базе: {total}")
    print(f"разрешённых чатов: {len(allowed)}")
    for chat_id, title in allowed.items():
        count = connection.execute(
            "SELECT COUNT(*) FROM messages WHERE chat_id = ?", (chat_id,)
        ).fetchone()[0]
        print(f"  {chat_id}  {title or '—'}  ({count} сообщ.)")
    return 0


def list_chats() -> int:
    connection = connect()
    rows = connection.execute(
        "SELECT chat_id, COALESCE(title, '—'), COALESCE(kind, '—'), allowed, seen_at_utc "
        "FROM chats ORDER BY allowed DESC, seen_at_utc DESC"
    ).fetchall()
    if not rows:
        print("бот пока не видел ни одного чата — добавьте его в чат и запустите --once")
        return 0
    for chat_id, title, kind, allowed, seen in rows:
        mark = "собираем" if allowed else "не разрешён"
        print(f"{chat_id:>16}  {mark:<12} {kind:<10} {title}   (виден с {seen})")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--once", action="store_true", help="один проход и выход")
    parser.add_argument("--wait", type=int, default=25, help="секунд длинного опроса (по умолчанию 25)")
    parser.add_argument("--status", action="store_true", help="состояние без обращения к сети")
    parser.add_argument("--list-chats", action="store_true", help="чаты, которые видел бот, и их id")
    parser.add_argument("--version", action="version", version=VERSION)
    args = parser.parse_args(argv)

    try:
        if args.status:
            return status()
        if args.list_chats:
            return list_chats()
        return run(once=args.once, wait=args.wait)
    except Stop as error:
        print(f"остановка: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
