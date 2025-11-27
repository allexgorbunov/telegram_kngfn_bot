import os
import csv
import asyncio
import logging
from pathlib import Path
from aiohttp import web
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    AIORateLimiter,
    filters,
)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

TOKEN = os.getenv("TELEGRAM_TOKEN")
if not TOKEN:
    raise RuntimeError("TELEGRAM_TOKEN is not set")

BASE_URL = os.getenv("BASE_URL")
PARTICIPANTS_FILE = Path("participants.csv")

# Глобальный лок для синхронной записи/чтения
lock = asyncio.Lock()


async def load_participants() -> list[dict]:
    """Читаем CSV при старте / перезапуске."""
    if not PARTICIPANTS_FILE.exists():
        return []
    participants = []
    async with lock:
        with PARTICIPANTS_FILE.open("r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                participants.append({"id": row["id"], "email": row["email"]})
    return participants


async def save_participant(p: dict) -> None:
    """Безопасно дописываем участника в CSV."""
    file_exists = PARTICIPANTS_FILE.exists()
    async with lock:
        with PARTICIPANTS_FILE.open("a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["id", "email"])
            if not file_exists:
                writer.writeheader()
            writer.writerow(p)


# ---- Хэндлеры ----

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Введи свой email для участия в розыгрыше:")


async def handle_email(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    text = update.message.text.strip()

    if "@" not in text or "." not in text:
        await update.message.reply_text("Похоже, это не email. Попробуй ещё раз.")
        return

    # participants храним в bot_data и поддерживаем консистентность через lock
    async with lock:
        participants = context.application.bot_data.setdefault("participants", [])

        if any(p["email"] == text for p in participants):
            await update.message.reply_text("Этот email уже зарегистрирован.")
            return

        new_id = len(participants) + 1
        user_id = f"USER{new_id:03}"
        participant = {"id": user_id, "email": text}
        participants.append(participant)

        # запись в CSV
        await asyncio.to_thread(save_participant, participant)

    await update.message.reply_text(
        f"Ты успешно зарегистрирован! Твой ID для розыгрыша: {user_id}. "
        "Почта не будет показана никому."
    )


async def raffle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    import random

    async with lock:
        participants = context.application.bot_data.get("participants", [])

        if not participants:
            await update.message.reply_text("Нет участников для розыгрыша.")
            return

        winner = random.choice(participants)

    await update.message.reply_text(f"Победитель: ID {winner['id']}.")


# ---- Webhook-сервер на aiohttp ----

async def telegram_webhook(request: web.Request):
    app: ApplicationBuilder = request.app["bot_app"]
    data = await request.json()
    update = Update.de_json(data, app.bot)
    await app.update_queue.put(update)
    return web.Response(text="ok")


async def set_webhook_handler(request: web.Request):
    app: ApplicationBuilder = request.app["bot_app"]
    if not BASE_URL:
        return web.Response(text="BASE_URL is not set", status=500)
    url = f"{BASE_URL}/webhook"
    ok = await app.bot.set_webhook(url)
    text = f"Webhook set to {url}, ok={ok}"
    return web.Response(text=text)


async def healthcheck(request: web.Request):
    return web.Response(text="OK")


async def main():
    # Создаём Telegram Application c rate limiter'ом
    application = (
        ApplicationBuilder()
        .token(TOKEN)
        .rate_limiter(AIORateLimiter(max_retries=2))
        .build()
    )

    # Восстанавливаем участников из CSV при старте
    participants = await load_participants()
    application.bot_data["participants"] = participants

    # Регистрируем хэндлеры
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("raffle", raffle))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_email))

    # aiohttp-приложение
    web_app = web.Application()
    web_app["bot_app"] = application
    web_app.router.add_post("/webhook", telegram_webhook)
    web_app.router.add_get("/set_webhook", set_webhook_handler)
    web_app.router.add_get("/", healthcheck)

    # Запускаем Telegram-часть и HTTP-сервер параллельно
    runner = web.AppRunner(web_app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", int(os.getenv("PORT", "10000")))

    # start() — без polling, только обработка очереди
    await application.initialize()
    await application.start()
    await site.start()

    logger.info("Service started")

    # держим процесс живым (на час/сколько нужно)
    try:
        await asyncio.Event().wait()
    finally:
        await application.stop()
        await application.shutdown()
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
