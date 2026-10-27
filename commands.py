from __future__ import annotations

import asyncio
import logging
import re
from typing import Any, Awaitable, Callable

from sqlalchemy import func, select

from config import Settings, get_settings
from db import Group, Post, QueueItem, SendLog, session_scope, settings_store, utcnow
from eitaa_client import EitaaClient, EitaaError
from queue_worker import QueueWorker
from reporter import collect_stats, format_report

logger = logging.getLogger("eitaa.commands")

Handler = Callable[["CommandRouter", dict[str, Any], str], Awaitable[str]]

HELP_TEXT = """🤖 دستورهای مدیریتی ربات

/add <شناسه یا لینک گروه> — افزودن گروه
/remove <شناسه> — حذف گروه
/list — لیست گروه‌ها و وضعیت آن‌ها
/disable <شناسه> — غیرفعال‌سازی موقت گروه
/enable <شناسه> — فعال‌سازی مجدد گروه
/pause — توقف کل ارسال‌ها
/resume — ادامه‌ی ارسال‌ها
/delay <ثانیه> — تأخیر بین انتشار در کانال و ارسال به گروه‌ها
/rate <ثانیه> — فاصله‌ی بین دو ارسال متوالی
/perminute <عدد> — سقف ارسال در هر دقیقه
/status — وضعیت لحظه‌ای صف و ارسال‌ها
/report — گزارش امروز
/retry — تلاش مجدد برای ارسال‌های شکست‌خورده
/help — همین راهنما"""

JOIN_PATTERN = re.compile(r"(?:https?://)?(?:www\.)?eitaa\.com/joinchat/([A-Za-z0-9_+-]+)")
LINK_PATTERN = re.compile(r"(?:https?://)?(?:www\.)?eitaa\.com/([A-Za-z0-9_+-]+)")


def normalize_chat_id(raw: str) -> str | None:
    """Turn a numeric id, @username or eitaa.com link into a chat identifier."""
    value = raw.strip()
    if not value:
        return None
    join_match = JOIN_PATTERN.search(value)
    if join_match:
        # A joinchat hash is not a username; keep the invite link intact.
        return f"https://eitaa.com/joinchat/{join_match.group(1)}"
    match = LINK_PATTERN.search(value)
    if match:
        token = match.group(1)
        return token if token.startswith(("+", "-")) else f"@{token}"
    if value.startswith("@"):
        return value
    if re.fullmatch(r"-?\d+", value):
        return value
    if re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{3,}", value):
        return f"@{value}"
    return None


class CommandRouter:
    """Handles admin commands arriving in the bot's private chat."""

    def __init__(
        self,
        client: EitaaClient,
        worker: QueueWorker,
        queue: "asyncio.Queue[dict[str, Any]]",
        settings: Settings | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.client = client
        self.worker = worker
        self.queue = queue
        self._stop = asyncio.Event()
        self.handlers: dict[str, Handler] = {
            "/start": CommandRouter.cmd_help,
            "/help": CommandRouter.cmd_help,
            "/add": CommandRouter.cmd_add,
            "/remove": CommandRouter.cmd_remove,
            "/list": CommandRouter.cmd_list,
            "/disable": CommandRouter.cmd_disable,
            "/enable": CommandRouter.cmd_enable,
            "/pause": CommandRouter.cmd_pause,
            "/resume": CommandRouter.cmd_resume,
            "/delay": CommandRouter.cmd_delay,
            "/rate": CommandRouter.cmd_rate,
            "/perminute": CommandRouter.cmd_perminute,
            "/status": CommandRouter.cmd_status,
            "/report": CommandRouter.cmd_report,
            "/retry": CommandRouter.cmd_retry,
        }

    def stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        while not self._stop.is_set():
            try:
                message = await asyncio.wait_for(self.queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            try:
                await self.handle_message(message)
            except Exception:
                logger.exception("failed to handle admin message")

    async def handle_message(self, message: dict[str, Any]) -> None:
        text = (message.get("text") or "").strip()
        if not text.startswith("/"):
            return
        sender = message.get("from") or {}
        chat = message.get("chat") or {}
        user_id = int(sender.get("id") or chat.get("id") or 0)
        reply_to = str(chat.get("id") or user_id)

        if user_id not in self.settings.admin_ids:
            logger.warning("ignoring command from non-admin %s", user_id)
            return

        parts = text.split(maxsplit=1)
        command = parts[0].split("@", 1)[0].lower()
        argument = parts[1].strip() if len(parts) > 1 else ""

        handler = self.handlers.get(command)
        if handler is None:
            await self._reply(reply_to, "دستور ناشناخته است. /help را بفرستید.")
            return
        try:
            answer = await handler(self, message, argument)
        except Exception as exc:
            logger.exception("command %s failed", command)
            answer = f"خطا در اجرای دستور: {exc}"
        await self._reply(reply_to, answer)

    async def _reply(self, chat_id: str, text: str) -> None:
        try:
            await self.client.send_message(chat_id, text[:4000])
        except EitaaError as exc:
            logger.warning("could not reply to %s: %s", chat_id, exc.message)

    # ------------------------------------------------------------------ handlers

    async def cmd_help(self, message: dict[str, Any], argument: str) -> str:
        return HELP_TEXT

    async def cmd_add(self, message: dict[str, Any], argument: str) -> str:
        chat_id = normalize_chat_id(argument)
        if chat_id is None:
            return "شناسه‌ی گروه نامعتبر است. مثال: /add -1001234567890 یا /add https://eitaa.com/mygroup"

        title: str | None = None
        try:
            if not hasattr(self.client, "get_chat"):
                raise EitaaError("transport cannot resolve chat titles")
            info = await self.client.get_chat(chat_id)
            title = info.get("title") or info.get("username")
            resolved = info.get("id")
            if resolved is not None:
                chat_id = str(resolved)
        except EitaaError as exc:
            logger.info("getChat failed for %s: %s", chat_id, exc.message)

        async with session_scope() as session:
            existing = (
                await session.execute(select(Group).where(Group.chat_id == chat_id))
            ).scalar_one_or_none()
            if existing is not None:
                existing.enabled = True
                existing.status = "active"
                existing.last_error = None
                existing.fail_count = 0
                if title:
                    existing.title = title
                return f"گروه {chat_id} از قبل موجود بود و دوباره فعال شد."
            session.add(Group(chat_id=chat_id, title=title))
        return f"✅ گروه {title or chat_id} اضافه شد."

    async def _find_group(self, session: Any, argument: str) -> Group | None:
        chat_id = normalize_chat_id(argument) or argument.strip()
        return (
            await session.execute(select(Group).where(Group.chat_id == chat_id))
        ).scalar_one_or_none()

    async def cmd_remove(self, message: dict[str, Any], argument: str) -> str:
        async with session_scope() as session:
            group = await self._find_group(session, argument)
            if group is None:
                return "چنین گروهی در لیست نیست."
            await session.delete(group)
        return "🗑 گروه حذف شد."

    async def cmd_disable(self, message: dict[str, Any], argument: str) -> str:
        async with session_scope() as session:
            group = await self._find_group(session, argument)
            if group is None:
                return "چنین گروهی در لیست نیست."
            group.enabled = False
        return "⏸ گروه موقتاً غیرفعال شد."

    async def cmd_enable(self, message: dict[str, Any], argument: str) -> str:
        async with session_scope() as session:
            group = await self._find_group(session, argument)
            if group is None:
                return "چنین گروهی در لیست نیست."
            group.enabled = True
            group.status = "active"
            group.fail_count = 0
            group.last_error = None
        return "▶️ گروه فعال شد."

    async def cmd_list(self, message: dict[str, Any], argument: str) -> str:
        async with session_scope() as session:
            groups = (await session.execute(select(Group).order_by(Group.id))).scalars().all()
            rows = [
                (group.chat_id, group.title, group.enabled, group.status, group.last_error)
                for group in groups
            ]
        if not rows:
            return "لیست گروه‌ها خالی است. با /add گروه اضافه کنید."
        lines = [f"📋 تعداد گروه‌ها: {len(rows)}", ""]
        for chat_id, title, enabled, status, last_error in rows:
            icon = "✅" if enabled and status == "active" else ("⛔️" if not enabled else "⚠️")
            line = f"{icon} {title or '-'} | {chat_id} | {status}"
            if last_error and status != "active":
                line += f" | {last_error[:80]}"
            lines.append(line)
        return "\n".join(lines)

    async def cmd_pause(self, message: dict[str, Any], argument: str) -> str:
        await settings_store.set("paused", True)
        return "⏸ ارسال‌ها متوقف شد. برای ادامه /resume را بفرستید."

    async def cmd_resume(self, message: dict[str, Any], argument: str) -> str:
        await settings_store.set("paused", False)
        return "▶️ ارسال‌ها از سر گرفته شد."

    async def cmd_delay(self, message: dict[str, Any], argument: str) -> str:
        current = await settings_store.get_int("delay_seconds")
        if not argument:
            return f"تأخیر فعلی: {current} ثانیه. برای تغییر: /delay 90"
        try:
            value = int(float(argument))
        except ValueError:
            return "عدد نامعتبر است."
        if not 0 <= value <= 86400:
            return "مقدار باید بین ۰ تا ۸۶۴۰۰ ثانیه باشد."
        await settings_store.set("delay_seconds", value)
        return f"⏱ تأخیر ارسال روی {value} ثانیه تنظیم شد."

    async def cmd_rate(self, message: dict[str, Any], argument: str) -> str:
        current = await settings_store.get_float("send_interval")
        if not argument:
            return f"فاصله‌ی فعلی بین ارسال‌ها: {current} ثانیه. برای تغییر: /rate 1.5"
        try:
            value = float(argument)
        except ValueError:
            return "عدد نامعتبر است."
        if not 0.1 <= value <= 600:
            return "مقدار باید بین ۰.۱ تا ۶۰۰ ثانیه باشد."
        await settings_store.set("send_interval", value)
        return f"🚦 فاصله‌ی بین ارسال‌ها روی {value} ثانیه تنظیم شد."

    async def cmd_perminute(self, message: dict[str, Any], argument: str) -> str:
        current = await settings_store.get_int("max_per_minute")
        if not argument:
            return f"سقف فعلی ارسال در دقیقه: {current}. برای تغییر: /perminute 40"
        try:
            value = int(argument)
        except ValueError:
            return "عدد نامعتبر است."
        if not 1 <= value <= 600:
            return "مقدار باید بین ۱ تا ۶۰۰ باشد."
        await settings_store.set("max_per_minute", value)
        return f"📉 سقف ارسال در دقیقه روی {value} تنظیم شد."

    async def cmd_status(self, message: dict[str, Any], argument: str) -> str:
        paused = await settings_store.get_bool("paused")
        delay = await settings_store.get_int("delay_seconds")
        interval = await settings_store.get_float("send_interval")
        per_minute = await settings_store.get_int("max_per_minute")

        async with session_scope() as session:
            pending = (
                await session.execute(
                    select(func.count(QueueItem.id)).where(QueueItem.status == "pending")
                )
            ).scalar_one()
            sending = (
                await session.execute(
                    select(func.count(QueueItem.id)).where(QueueItem.status == "sending")
                )
            ).scalar_one()
            failed = (
                await session.execute(
                    select(func.count(QueueItem.id)).where(QueueItem.status == "failed")
                )
            ).scalar_one()
            waiting_posts = (
                await session.execute(
                    select(func.count(Post.id)).where(Post.dispatched.is_(False))
                )
            ).scalar_one()
            groups_enabled = (
                await session.execute(select(func.count(Group.id)).where(Group.enabled.is_(True)))
            ).scalar_one()
            last_ok = (
                await session.execute(
                    select(func.max(SendLog.created_at)).where(SendLog.ok.is_(True))
                )
            ).scalar_one()

        lines = [
            "📟 وضعیت ربات",
            "",
            f"• وضعیت: {'متوقف ⏸' if paused else 'فعال ▶️'}",
            f"• پست‌های در انتظار ارسال: {waiting_posts}",
            f"• آیتم‌های در صف: {pending} (در حال ارسال: {sending}) | شکست‌خورده: {failed}",
            f"• گروه‌های فعال: {groups_enabled}",
            f"• تأخیر: {delay}s | فاصله: {interval}s | سقف دقیقه: {per_minute}",
            f"• آخرین ارسال موفق: {last_ok:%Y-%m-%d %H:%M:%S} UTC" if last_ok else "• هنوز ارسالی انجام نشده",
        ]
        cooldown = self.worker.limiter.cooldown_remaining
        if cooldown > 0:
            lines.append(f"• در حال استراحت به دلیل محدودیت نرخ: {cooldown:.0f} ثانیه")
        if self.worker.recent_errors:
            lines.append("")
            lines.append("آخرین خطاها:")
            lines.extend(f"- {error}" for error in list(self.worker.recent_errors)[-5:])
        return "\n".join(lines)

    async def cmd_report(self, message: dict[str, Any], argument: str) -> str:
        stats = await collect_stats()
        return format_report(stats)

    async def cmd_retry(self, message: dict[str, Any], argument: str) -> str:
        async with session_scope() as session:
            items = (
                await session.execute(select(QueueItem).where(QueueItem.status == "failed"))
            ).scalars().all()
            for item in items:
                item.status = "pending"
                item.attempts = 0
                item.next_attempt_at = utcnow()
                item.last_error = None
        return f"🔁 {len(items)} ارسال شکست‌خورده دوباره به صف اضافه شد."


# Shared group mutations used by manage.py CLI and the web panel so behaviour
# never diverges. These match manage.py cmd_add / cmd_remove / _set_enabled.


async def add_managed_group(raw: str, title: str | None = None) -> tuple[int, str]:
    chat_id = normalize_chat_id(raw) or raw.strip()
    async with session_scope() as session:
        existing = (
            await session.execute(select(Group).where(Group.chat_id == chat_id))
        ).scalar_one_or_none()
        if existing is not None:
            existing.enabled = True
            existing.status = "active"
            existing.last_error = None
            existing.fail_count = 0
            if title:
                existing.title = title
            return 0, f"group {chat_id} already existed; re-enabled"
        session.add(Group(chat_id=chat_id, title=title))
    return 0, f"added {chat_id}"


async def remove_managed_group(raw: str) -> tuple[int, str]:
    chat_id = normalize_chat_id(raw) or raw.strip()
    async with session_scope() as session:
        group = (
            await session.execute(select(Group).where(Group.chat_id == chat_id))
        ).scalar_one_or_none()
        if group is None:
            return 1, "not found"
        await session.delete(group)
    return 0, f"removed {chat_id}"


async def set_managed_group_enabled(raw: str, enabled: bool) -> tuple[int, str]:
    chat_id = normalize_chat_id(raw) or raw.strip()
    async with session_scope() as session:
        group = (
            await session.execute(select(Group).where(Group.chat_id == chat_id))
        ).scalar_one_or_none()
        if group is None:
            return 1, "not found"
        group.enabled = enabled
        if enabled:
            group.status = "active"
            group.fail_count = 0
            group.last_error = None
    return 0, ("enabled " if enabled else "disabled ") + chat_id
