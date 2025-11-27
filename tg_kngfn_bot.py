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

ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))

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
                """
                INSERT INTO participants (email)
                VALUES (%s)
                ON CONFLICT (email) DO NOTHING
                RETURNING id;
                """,
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
    # только админ может вызывать розыгрыш
    user_id = update.effective_user.id if update.effective_user else None
    if user_id != ADMIN_ID:
        await update.message.reply_text("Команда недоступна.")
        return

    async with lock:
        winner_code = await pick_random_winner()

    if not winner
