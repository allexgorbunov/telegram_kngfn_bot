import os
import asyncio
import logging
import random
import io
import csv

from aiohttp import web
import psycopg2
from psycopg2.extras import RealDictCursor

from telegram import Update, InputFile
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


async def pick_random_winner
