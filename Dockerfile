FROM python:3.12-slim

WORKDIR /app
COPY tg_chat_collector.py .

# Данные держим на томе: файловая система контейнера эфемерна, а вместе с базой
# теряется смещение очереди — сообщения за время простоя Телеграм хранит только сутки.
ENV DB_PATH=/data/chats.sqlite3
VOLUME ["/data"]

# Воркер, а не веб-сервис: порт не слушает, HTTP-healthcheck не пройдёт.
CMD ["python", "-u", "tg_chat_collector.py"]
