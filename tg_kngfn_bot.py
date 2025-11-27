import os
import asyncio
import logging
import random

from aiohttp import web
import psycopg2
from psycopg2.extras import RealDictCursor

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
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")

lock = asyncio.Lock()


def get_conn():
    return psycopg2.connect(DATABASE_URL)


def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS participants (
            id SERIAL PRIMARY KEY,
            email TEXT UNIQUE NOT NULL
        );
        """
    )
    conn.commit()
    cur.close()
    conn.close()


async def add_participant(email: str) -> str | None:
    """Добавить участника, вернуть его ID вида USERXXX или None, если уже есть."""
    def _inner():
        conn = get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                "INSERT INTO participants (email) VALUES (%s) "
                "ON CONFLICT (email) DO NOTHING "
                "RETURNING id;",
                (email,),
            )
            row = cur.fetchone()
            conn.commit()
        finally:
            cur.close()
            conn.close()
        return row[0] if row else None

    new_id = await asyncio.to_thread(_inner)
    if new_id is None:
        return None
    return f"USER{new_id:03}"


async def pick_random_winner() -> str | None:
    """Вернуть ID победителя (USERXXX) или None, если никого нет."""
    def _inner():
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT id FROM participants;")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows

    rows = await asyncio.to_thread(_inner)
    if not rows:
        return None
    winner_row = random.choice(rows)
    return f"USER{winner_row['id']:03}"


# ---------- handlers ----------

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Введи свой email для участия в розыгрыше:")


async def handle_email(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    text = update.message.text.strip()

    if "@" not in text or "." not in text:
        await update.message.reply_text("Похоже, это не email. Попробуй ещё раз.")
        return

    async with lock:
        user_code = await add_participant(text)

    if user_code is None:
        await update.message.reply_text("Этот email уже зарегистрирован.")
        return

    await update.message.reply_text(
        f"Ты успешно зарегистрирован! Твой ID для розыгрыша: {user_code}. "
        "Почта не будет показана никому."
    )


async def raffle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    async with lock:
        winner_code = await pick_random_winner()

    if not winner_code:
        await update.message.reply_text("Нет участников для розыгрыша.")
        return

    await update.message.reply_text(f"Победитель: ID {winner_code}.")


# ---------- webhook server (aiohttp) ----------

async def telegram_webhook(request: web.Request):
    app = request.app["bot_app"]
    data = await request.json()
    update = Update.de_json(data, app.bot)
    await app.update_queue.put(update)
    return web.Response(text="ok")


async def set_webhook_handler(request: web.Request):
    app = request.app["bot_app"]
    if not BASE_URL:
        return web.Response(text="BASE_URL is not set", status=500)
    url = f"{BASE_URL}/webhook"
    ok = await app.bot.set_webhook(url)
    return web.Response(text=f"Webhook set to {url}, ok={ok}")


async def healthcheck(request: web.Request):
    return web.Response(text="OK")


async def main():
    # инициализируем БД (создаём таблицу, если её нет)
    await asyncio.to_thread(init_db)

    application = (
        ApplicationBuilder()
        .token(TOKEN)
        .rate_limiter(AIORateLimiter(max_retries=2))
        .build()
    )

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("raffle", raffle))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_email))

    web_app = web.Application()
    web_app["bot_app"] = application
    web_app.router.add_post("/webhook", telegram_webhook)
    web_app.router.add_get("/set_webhook", set_webhook_handler)
    web_app.router.add_get("/", healthcheck)

    runner = web.AppRunner(web_app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", int(os.getenv("PORT", "10000")))

    await application.initialize()
    await application.start()
    await site.start()

    logger.info("Service with DB started")

    try:
        await asyncio.Event().wait()
    finally:
        await application.stop()
        await application.shutdown()
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
