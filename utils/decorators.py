from __future__ import annotations

import functools
import time
from datetime import datetime, timezone

from telegram import Update
from telegram.ext import ContextTypes

from config import settings


def dm_only(func):
    """Handler works only in private messages."""
    @functools.wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        if update.effective_chat and update.effective_chat.type != "private":
            await update.message.reply_text(
                "Эта команда работает только в личных сообщениях \U0001f449 напиши мне в DM"
            )
            return
        return await func(update, context, *args, **kwargs)
    return wrapper


def group_only(func):
    """Handler works only in group chats."""
    @functools.wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        if update.effective_chat and update.effective_chat.type == "private":
            await update.message.reply_text(
                "Эта команда работает только в групповом чате."
            )
            return
        return await func(update, context, *args, **kwargs)
    return wrapper


def registered_only(func):
    """Check that user is registered."""
    @functools.wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        db = context.bot_data.get("db")
        if db is None:
            return await func(update, context, *args, **kwargs)
        user = await db.get_user(update.effective_user.id)
        if user is None:
            await update.message.reply_text(
                "Ты ещё не в лиге! Используй /join чтобы присоединиться."
            )
            return
        return await func(update, context, *args, **kwargs)
    return wrapper


def admin_only(func):
    """Check that user is in ADMIN_IDS."""
    @functools.wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        if update.effective_user.id not in settings.ADMIN_IDS:
            await update.message.reply_text("Эта команда только для администраторов.")
            return
        return await func(update, context, *args, **kwargs)
    return wrapper


def check_deadline(session: str = "qualifying"):
    """Check that the deadline hasn't passed."""
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
            db = context.bot_data.get("db")
            if db is None:
                return await func(update, context, *args, **kwargs)
            race = await db.get_next_race()
            if race is None:
                await update.message.reply_text("Нет запланированных гонок.")
                return
            deadline_str = (
                race.qualifying_datetime if session == "qualifying" else race.race_datetime
            )
            deadline = datetime.fromisoformat(deadline_str)
            if datetime.now(timezone.utc).replace(tzinfo=None) > deadline:
                await update.message.reply_text(
                    "\u26a0\ufe0f Дедлайн прошёл! Изменения заблокированы."
                )
                return
            return await func(update, context, *args, **kwargs)
        return wrapper
    return decorator


def rate_limit(seconds: int = 5):
    """Simple per-user rate limit."""
    def decorator(func):
        _last_call: dict[int, float] = {}

        @functools.wraps(func)
        async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
            user_id = update.effective_user.id
            now = time.time()
            if user_id in _last_call and now - _last_call[user_id] < seconds:
                return
            _last_call[user_id] = now
            return await func(update, context, *args, **kwargs)
        return wrapper
    return decorator
