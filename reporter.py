from __future__ import annotations

import asyncio
import datetime as dt
import logging
from typing import Any

from sqlalchemy import func, select

from config import Settings, get_settings
from db import Group, Post, QueueItem, SendLog, session_scope, settings_store, utcnow
from eitaa_client import EitaaClient, EitaaError

logger = logging.getLogger("eitaa.reporter")


async def notify_admins(client: EitaaClient, text: str) -> None:
    """Best-effort broadcast to every configured admin id."""
    settings = get_settings()
    for admin_id in settings.admin_ids:
        try:
            await client.send_message(str(admin_id), text)
        except EitaaError as exc:
            logger.warning("could not notify admin %s: %s", admin_id, exc.message)
        except Exception:
            logger.exception("unexpected error notifying admin %s", admin_id)


def _local_day_bounds(settings: Settings, day: dt.date) -> tuple[dt.datetime, dt.datetime]:
    start_local = dt.datetime.combine(day, dt.time.min, tzinfo=settings.tz)
    end_local = start_local + dt.timedelta(days=1)
    to_naive_utc = lambda value: value.astimezone(dt.timezone.utc).replace(tzinfo=None)
    return to_naive_utc(start_local), to_naive_utc(end_local)


async def collect_stats(day: dt.date | None = None) -> dict[str, Any]:
    settings = get_settings()
    day = day or dt.datetime.now(settings.tz).date()
    start, end = _local_day_bounds(settings, day)

    async with session_scope() as session:
        posts = (
            await session.execute(
                select(func.count(Post.id)).where(Post.created_at >= start, Post.created_at < end)
            )
        ).scalar_one()
        ok_count = (
            await session.execute(
                select(func.count(SendLog.id)).where(
                    SendLog.created_at >= start, SendLog.created_at < end, SendLog.ok.is_(True)
                )
            )
        ).scalar_one()
        fail_count = (
            await session.execute(
                select(func.count(SendLog.id)).where(
                    SendLog.created_at >= start, SendLog.created_at < end, SendLog.ok.is_(False)
                )
            )
        ).scalar_one()
        pending = (
            await session.execute(
                select(func.count(QueueItem.id)).where(QueueItem.status.in_(["pending", "sending"]))
            )
        ).scalar_one()
        failed_items = (
            await session.execute(
                select(func.count(QueueItem.id)).where(QueueItem.status == "failed")
            )
        ).scalar_one()
        groups_total = (await session.execute(select(func.count(Group.id)))).scalar_one()
        groups_enabled = (
            await session.execute(select(func.count(Group.id)).where(Group.enabled.is_(True)))
        ).scalar_one()
        problem_groups = (
            await session.execute(
                select(Group)
                .where(Group.status != "active")
                .order_by(Group.last_error_at.desc())
                .limit(30)
            )
        ).scalars().all()
        last_send = (
            await session.execute(
                select(func.max(SendLog.created_at)).where(SendLog.ok.is_(True))
            )
        ).scalar_one()

        problems = [
            {
                "chat_id": group.chat_id,
                "title": group.title,
                "status": group.status,
                "enabled": group.enabled,
                "error": (group.last_error or "")[:160],
            }
            for group in problem_groups
        ]

    return {
        "day": day,
        "posts": posts,
        "sent_ok": ok_count,
        "sent_failed": fail_count,
        "queue_pending": pending,
        "queue_failed": failed_items,
        "groups_total": groups_total,
        "groups_enabled": groups_enabled,
        "problem_groups": problems,
        "last_success_at": last_send,
    }


def format_report(stats: dict[str, Any]) -> str:
    lines = [
        f"📊 گزارش روز {stats['day'].isoformat()}",
        "",
        f"• پست‌های دریافت‌شده: {stats['posts']}",
        f"• ارسال موفق: {stats['sent_ok']}",
        f"• ارسال ناموفق: {stats['sent_failed']}",
        f"• در صف: {stats['queue_pending']} | شکست‌خورده نهایی: {stats['queue_failed']}",
        f"• گروه‌ها: {stats['groups_enabled']} فعال از {stats['groups_total']}",
    ]
    if stats["last_success_at"]:
        lines.append(f"• آخرین ارسال موفق: {stats['last_success_at']:%Y-%m-%d %H:%M:%S} UTC")
    problems = stats["problem_groups"]
    if problems:
        lines.append("")
        lines.append("⚠️ گروه‌های مشکل‌دار:")
        for group in problems:
            label = group["title"] or group["chat_id"]
            lines.append(f"- {label} ({group['chat_id']}) [{group['status']}]: {group['error']}")
    else:
        lines.append("")
        lines.append("✅ هیچ گروه مشکل‌داری وجود ندارد.")
    return "\n".join(lines)


class Reporter:
    """Sends the automatic daily report at the configured local time."""

    def __init__(self, client: EitaaClient, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self.client = client
        self._stop = asyncio.Event()

    def stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        hour, minute = self.settings.report_hour_minute
        while not self._stop.is_set():
            try:
                now = dt.datetime.now(self.settings.tz)
                today = now.date().isoformat()
                due = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
                already = await settings_store.get_str("last_report_date")
                if now >= due and already != today:
                    await self.send_report(now.date())
                    await settings_store.set("last_report_date", today)
            except Exception:
                logger.exception("daily report loop failed")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=30.0)
            except asyncio.TimeoutError:
                continue

    async def send_report(self, day: dt.date) -> None:
        stats = await collect_stats(day)
        await notify_admins(self.client, format_report(stats))
        logger.info("daily report sent for %s", day)
