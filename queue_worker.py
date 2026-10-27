from __future__ import annotations

import asyncio
import datetime as dt
import logging
import random
import time
from collections import deque
from typing import Any

from sqlalchemy import select, update

from config import Settings, get_settings, is_within_active_window
from db import (
    Group,
    Post,
    PostMedia,
    QueueItem,
    RepeatItem,
    SendLog,
    session_scope,
    settings_store,
    utcnow,
)
from eitaa_client import ALBUM_TYPES, EitaaClient, EitaaError, RateLimitError
from web_transport import SessionUnrecoverable

logger = logging.getLogger("eitaa.queue")

# repeat_items.round values from repeater.py's idle-repeat cycle are small
# sequential integers (0, 1, 2, ...). Context-bundle rounds below are keyed to
# the triggering post's id instead, offset well clear of that range so the
# two independent repeat mechanisms never collide on (post_id, group_id, round).
BUNDLE_ROUND_OFFSET = 1_000_000_000


class RateLimiter:
    """Spacing + per-minute cap + a global cooldown honoured after HTTP 429."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._history: deque[float] = deque()
        self._cooldown_until: float = 0.0
        self._last_send: float = 0.0

    @property
    def cooldown_remaining(self) -> float:
        return max(0.0, self._cooldown_until - time.monotonic())

    def apply_cooldown(self, seconds: float) -> None:
        self._cooldown_until = max(self._cooldown_until, time.monotonic() + seconds)

    async def acquire(self, interval: float, max_per_minute: int) -> None:
        while True:
            now = time.monotonic()
            if now < self._cooldown_until:
                await asyncio.sleep(min(self._cooldown_until - now, 5.0))
                continue

            while self._history and now - self._history[0] > 60.0:
                self._history.popleft()
            if max_per_minute > 0 and len(self._history) >= max_per_minute:
                await asyncio.sleep(max(0.5, 60.0 - (now - self._history[0])))
                continue

            # Randomised spacing keeps the traffic pattern from looking robotic.
            jitter = 1.0 + random.uniform(-self.settings.send_jitter_ratio, self.settings.send_jitter_ratio)
            wait = (interval * max(jitter, 0.1)) - (now - self._last_send)
            if wait > 0:
                await asyncio.sleep(wait)
                continue

            self._last_send = time.monotonic()
            self._history.append(self._last_send)
            return


class QueueWorker:
    """Dispatches due posts into per-group queue items and delivers them."""

    def __init__(self, client: EitaaClient, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self.client = client
        self.limiter = RateLimiter(self.settings)
        self._stop = asyncio.Event()
        self.last_sent_at: dt.datetime | None = None
        self.recent_errors: deque[str] = deque(maxlen=10)

    def stop(self) -> None:
        self._stop.set()

    async def _sleep(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass

    # --------------------------------------------------------------- dispatcher

    async def dispatcher_loop(self) -> None:
        while not self._stop.is_set():
            try:
                if not is_within_active_window(self.settings):
                    await self._sleep(5.0)
                    continue
                await self.dispatch_due_posts()
            except Exception:
                logger.exception("dispatcher iteration failed")
            await self._sleep(2.0)

    async def dispatch_due_posts(self) -> int:
        created = 0
        async with session_scope() as session:
            posts = (
                await session.execute(
                    select(Post)
                    .where(Post.dispatched.is_(False), Post.ready_at <= utcnow())
                    .order_by(Post.id)
                    .limit(20)
                )
            ).scalars().all()
            if not posts:
                return 0

            groups = (
                await session.execute(
                    select(Group).where(Group.enabled.is_(True), Group.status != "kicked")
                )
            ).scalars().all()

            for post in posts:
                existing = set(
                    (
                        await session.execute(
                            select(QueueItem.group_id).where(QueueItem.post_id == post.id)
                        )
                    ).scalars().all()
                )
                for group in groups:
                    if group.id in existing:
                        continue  # idempotency: never queue the same pair twice
                    session.add(QueueItem(post_id=post.id, group_id=group.id))
                    created += 1
                post.dispatched = True
                post.dispatched_at = utcnow()
                logger.info("dispatched post %s to %d groups", post.id, len(groups))

                await self._cancel_pending_idle_repeats(session)
                if self.settings.bundle_enabled and post.message_id and post.message_id > 0:
                    await self._queue_context_bundle(session, post, groups)
        return created

    async def _cancel_pending_idle_repeats(self, session: Any) -> int:
        """A genuinely new channel post supersedes any not-yet-sent idle-repeat
        batch — the client wants the old batch dropped, not delivered alongside
        the new post. Only touches idle-repeater rows (round < BUNDLE_ROUND_OFFSET);
        context-bundle rows for THIS post are added after this call and must
        survive it untouched.
        """
        result = await session.execute(
            update(RepeatItem)
            .where(
                RepeatItem.status == "pending",
                RepeatItem.round < BUNDLE_ROUND_OFFSET,
            )
            .values(status="skipped", last_error="cancelled: newer channel post arrived")
        )
        cancelled = int(result.rowcount or 0)
        if cancelled:
            logger.info(
                "cancelled %d pending idle-repeat items: newer post superseded them",
                cancelled,
            )
        return cancelled

    async def _queue_context_bundle(self, session: Any, post: Post, groups: list[Group]) -> None:
        """Re-sends the N most recent predecessors alongside a new post.

        Uses RepeatItem (not QueueItem) on purpose: QueueItem's uniqueness
        means a predecessor already delivered once would never be queued
        again for the same group, which is exactly the opposite of what this
        feature wants — every new post should bundle fresh copies of its
        recent history, not skip posts already sent.
        """
        n = self.settings.bundle_previous_count
        if n <= 0:
            return
        predecessors = (
            await session.execute(
                select(Post)
                .where(Post.id < post.id, Post.message_id > 0)
                .order_by(Post.id.desc())
                .limit(n)
            )
        ).scalars().all()
        if not predecessors:
            return

        round_ = BUNDLE_ROUND_OFFSET + post.id
        created = 0
        for predecessor in predecessors:
            for group in groups:
                exists = (
                    await session.execute(
                        select(RepeatItem.id).where(
                            RepeatItem.post_id == predecessor.id,
                            RepeatItem.group_id == group.id,
                            RepeatItem.round == round_,
                        )
                    )
                ).scalar_one_or_none()
                if exists is not None:
                    continue
                session.add(RepeatItem(post_id=predecessor.id, group_id=group.id, round=round_))
                created += 1
        logger.info(
            "context bundle for post %s: queued %d items (%d predecessors x %d groups)",
            post.id, created, len(predecessors), len(groups),
        )

    # -------------------------------------------------------------------- sender

    async def sender_loop(self) -> None:
        while not self._stop.is_set():
            try:
                if not is_within_active_window(self.settings):
                    await self._sleep(5.0)
                    continue
                if await settings_store.get_bool("paused"):
                    await self._sleep(3.0)
                    continue
                item = await self._claim_next_item()
                if item is None:
                    await self._sleep(1.0)
                    continue
                interval = await settings_store.get_float("send_interval")
                max_per_minute = await settings_store.get_int("max_per_minute")
                await self.limiter.acquire(max(interval, 0.05), max_per_minute)
                if hasattr(self.client, "ensure_session"):
                    await self.client.ensure_session()
                await self._deliver(item)
            except asyncio.CancelledError:
                raise
            except SessionUnrecoverable:
                raise
            except Exception:
                logger.exception("sender iteration failed")
                await self._sleep(2.0)

    # ---------------------------------------------------------- repeat sender

    async def repeat_sender_loop(self) -> None:
        """Delivers RepeatItem rows created by repeater.py.

        Shares self.limiter with sender_loop so idle-channel repeats never
        push total outbound traffic above the configured rate — they simply
        take a turn alongside normal deliveries.
        """
        while not self._stop.is_set():
            try:
                if not is_within_active_window(self.settings):
                    await self._sleep(5.0)
                    continue
                if await settings_store.get_bool("paused"):
                    await self._sleep(3.0)
                    continue
                item = await self._claim_next_repeat_item()
                if item is None:
                    await self._sleep(2.0)
                    continue
                interval = await settings_store.get_float("send_interval")
                max_per_minute = await settings_store.get_int("max_per_minute")
                await self.limiter.acquire(max(interval, 0.05), max_per_minute)
                if hasattr(self.client, "ensure_session"):
                    await self.client.ensure_session()
                await self._deliver_repeat(item)
            except asyncio.CancelledError:
                raise
            except SessionUnrecoverable:
                raise
            except Exception:
                logger.exception("repeat sender iteration failed")
                await self._sleep(2.0)

    async def _claim_next_repeat_item(self) -> RepeatItem | None:
        async with session_scope() as session:
            item = (
                await session.execute(
                    select(RepeatItem)
                    .where(RepeatItem.status == "pending", RepeatItem.next_attempt_at <= utcnow())
                    .order_by(RepeatItem.next_attempt_at, RepeatItem.id)
                    .limit(1)
                )
            ).scalar_one_or_none()
            if item is None:
                return None
            item.status = "sending"
            item.attempts += 1
            return item

    async def _deliver_repeat(self, item: RepeatItem) -> None:
        async with session_scope() as session:
            post = await session.get(Post, item.post_id)
            group = await session.get(Group, item.group_id)
            media_message_ids = [
                row.message_id
                for row in (
                    await session.execute(
                        select(PostMedia)
                        .where(PostMedia.post_id == item.post_id)
                        .order_by(PostMedia.position)
                    )
                ).scalars().all()
                if row.message_id
            ]
            post_source_chat_id = post.source_chat_id if post else None
            post_message_id = post.message_id if post else None
            chat_id = group.chat_id if group else None

        if not (post and group and chat_id and post_source_chat_id and post_message_id and post_message_id > 0):
            await self._finalize_repeat(item, ok=False, error="repeat post/group vanished")
            return
        if not hasattr(self.client, "forward_message"):
            await self._finalize_repeat(item, ok=False, error="transport cannot forward")
            return

        try:
            ids = media_message_ids or [post_message_id]
            await self.client.forward_message(post_source_chat_id, ids, chat_id)
        except RateLimitError as exc:
            self.limiter.apply_cooldown(exc.retry_after)
            async with session_scope() as session:
                row = await session.get(RepeatItem, item.id)
                if row is not None:
                    row.status = "pending"
                    row.attempts = max(0, row.attempts - 1)
                    row.next_attempt_at = utcnow() + dt.timedelta(seconds=exc.retry_after + 1)
            return
        except EitaaError as exc:
            await self._finalize_repeat(item, ok=False, error=exc.message, permanent=exc.permanent)
            return
        except Exception as exc:
            logger.exception("unexpected repeat-forward failure for group %s", chat_id)
            await self._finalize_repeat(item, ok=False, error=f"unexpected: {exc}")
            return

        await self._finalize_repeat(item, ok=True, error=None)

    async def _finalize_repeat(
        self, item: RepeatItem, *, ok: bool, error: str | None, permanent: bool = False
    ) -> None:
        # Deliberately does not touch Group.status/enabled — the normal queue
        # already owns group health tracking; repeats just log their own outcome.
        async with session_scope() as session:
            row = await session.get(RepeatItem, item.id)
            if row is None:
                return
            if ok:
                row.status = "done"
                row.sent_at = utcnow()
                row.last_error = None
            else:
                row.last_error = (error or "unknown error")[:1000]
                if permanent or row.attempts >= self.settings.max_retries:
                    row.status = "failed"
                else:
                    row.status = "pending"
                    backoff = self.settings.retry_base_delay_seconds * (2 ** (row.attempts - 1))
                    row.next_attempt_at = utcnow() + dt.timedelta(seconds=min(backoff, 3600))
        if not ok:
            self.recent_errors.append(f"repeat: {error}"[:150])

    async def _claim_next_item(self) -> QueueItem | None:
        async with session_scope() as session:
            item = (
                await session.execute(
                    select(QueueItem)
                    .where(QueueItem.status == "pending", QueueItem.next_attempt_at <= utcnow())
                    .order_by(QueueItem.next_attempt_at, QueueItem.id)
                    .limit(1)
                )
            ).scalar_one_or_none()
            if item is None:
                return None
            item.status = "sending"
            item.attempts += 1
            return item

    async def _deliver(self, item: QueueItem) -> None:
        async with session_scope() as session:
            post = await session.get(Post, item.post_id)
            group = await session.get(Group, item.group_id)
            media = (
                await session.execute(
                    select(PostMedia)
                    .where(PostMedia.post_id == item.post_id)
                    .order_by(PostMedia.position)
                )
            ).scalars().all()
            payload = [
                {
                    "media_type": row.media_type,
                    "file_id": row.file_id,
                    "local_path": row.local_path,
                    "file_name": row.file_name,
                    "caption": row.caption,
                }
                for row in media
            ]
            post_text = post.text if post else None
            post_kind = post.kind if post else "text"
            post_source_chat_id = post.source_chat_id if post else None
            post_message_id = post.message_id if post else None
            media_message_ids = [row.message_id for row in media if row.message_id]
            chat_id = group.chat_id if group else None

        if post is None or group is None or chat_id is None:
            await self._finalize(item, ok=False, error="post or group vanished", permanent=True)
            return

        # A post captured by polling the real channel (web_ingest.py) carries a
        # positive, genuine message_id and can be forwarded natively — no
        # re-upload, no quality loss, real album grouping preserved. Posts
        # queued through manage.py publish use a negative synthetic id and have
        # no source message to forward, so they fall back to reconstruction.
        can_forward = (
            hasattr(self.client, "forward_message")
            and post_message_id is not None
            and post_message_id > 0
            and post_source_chat_id
        )

        started = time.monotonic()
        try:
            if can_forward:
                ids = media_message_ids or [post_message_id]
                await self.client.forward_message(post_source_chat_id, ids, chat_id)
            else:
                await self._send_payload(chat_id, post_kind, post_text, payload)
        except RateLimitError as exc:
            self.limiter.apply_cooldown(exc.retry_after)
            await self._requeue_after_rate_limit(item, exc.retry_after)
            self.recent_errors.append(f"429 (retry_after={exc.retry_after:.0f}s)")
            logger.warning("429 from Eitaa; cooling down %.1fs", exc.retry_after)
            return
        except EitaaError as exc:
            latency = (time.monotonic() - started) * 1000
            await self._finalize(
                item, ok=False, error=exc.message, permanent=exc.permanent, latency_ms=latency
            )
            self.recent_errors.append(f"{chat_id}: {exc.message[:120]}")
            return
        except Exception as exc:  # never let one bad send kill the worker
            logger.exception("unexpected send failure for group %s", chat_id)
            await self._finalize(item, ok=False, error=f"unexpected: {exc}", permanent=False)
            self.recent_errors.append(f"{chat_id}: unexpected {exc}")
            return

        latency = (time.monotonic() - started) * 1000
        await self._finalize(item, ok=True, error=None, permanent=False, latency_ms=latency)
        self.last_sent_at = utcnow()

    async def _send_payload(
        self,
        chat_id: str,
        kind: str,
        text: str | None,
        media: list[dict[str, Any]],
    ) -> None:
        """Re-send (copy) the post content.

        Eitaa exposes no reliable forward/copy endpoint for bots, so content is
        rebuilt from the stored file_id / cached file and sent as a fresh message.
        """
        if not media:
            if not text:
                raise EitaaError("empty post", permanent=True)
            await self.client.send_message(chat_id, text)
            return

        # eitaayar exposes no album endpoint at all, so do not even try there.
        if (
            self.client.is_botapi
            and len(media) > 1
            and all(row["media_type"] in ALBUM_TYPES for row in media)
        ):
            try:
                await self.client.send_media_group(chat_id, media)
                if text and not any(row.get("caption") for row in media):
                    await self.client.send_message(chat_id, text)
                return
            except RateLimitError:
                raise
            except EitaaError as exc:
                # A permanent error (kicked, no write access) will not improve by
                # sending the items one by one; only retry the unsupported case.
                if exc.permanent:
                    raise
                logger.warning("album send failed (%s); falling back to sequential", exc.message)

        first = True
        for row in media:
            caption = row.get("caption")
            if first and not caption and text:
                caption = text
            await self.client.send_media(
                chat_id,
                row["media_type"],
                file_id=row.get("file_id"),
                local_path=row.get("local_path"),
                caption=caption,
                file_name=row.get("file_name"),
            )
            first = False

    # ---------------------------------------------------------------- bookkeeping

    async def _requeue_after_rate_limit(self, item: QueueItem, retry_after: float) -> None:
        async with session_scope() as session:
            row = await session.get(QueueItem, item.id)
            if row is None:
                return
            row.status = "pending"
            # A 429 is the gateway's fault, not the item's: do not burn an attempt.
            row.attempts = max(0, row.attempts - 1)
            row.next_attempt_at = utcnow() + dt.timedelta(seconds=retry_after + 1)
            row.last_error = f"rate limited, retry after {retry_after:.0f}s"

    async def _finalize(
        self,
        item: QueueItem,
        *,
        ok: bool,
        error: str | None,
        permanent: bool,
        latency_ms: float | None = None,
    ) -> None:
        notify: tuple[str, str] | None = None
        async with session_scope() as session:
            row = await session.get(QueueItem, item.id)
            if row is None:
                return
            group = await session.get(Group, row.group_id)

            if ok:
                row.status = "done"
                row.sent_at = utcnow()
                row.last_error = None
                if group is not None:
                    group.fail_count = 0
                    group.last_sent_at = utcnow()
                    if group.status != "active":
                        group.status = "active"
                        group.last_error = None
            else:
                row.last_error = (error or "unknown error")[:1000]
                max_retries = self.settings.max_retries
                if permanent or row.attempts >= max_retries:
                    row.status = "failed"
                else:
                    row.status = "pending"
                    backoff = self.settings.retry_base_delay_seconds * (2 ** (row.attempts - 1))
                    row.next_attempt_at = utcnow() + dt.timedelta(
                        seconds=min(backoff, 3600) + random.uniform(0, 5)
                    )
                if group is not None:
                    group.fail_count += 1
                    group.last_error = row.last_error
                    group.last_error_at = utcnow()
                    if permanent:
                        group.status = "kicked"
                        group.enabled = False
                        notify = (group.chat_id, row.last_error or "")
                    elif group.fail_count >= 10 and group.status == "active":
                        group.status = "error"

            session.add(
                SendLog(
                    queue_item_id=row.id,
                    post_id=row.post_id,
                    group_id=row.group_id,
                    ok=ok,
                    error=None if ok else row.last_error,
                    latency_ms=latency_ms,
                )
            )

        if notify is not None:
            chat_id, reason = notify
            from reporter import notify_admins  # local import avoids a cycle

            await notify_admins(
                self.client,
                f"⚠️ گروه {chat_id} به صورت خودکار غیرفعال شد.\nدلیل: {reason}",
            )

    # -------------------------------------------------------------------- status

    async def discard_backlog(self) -> int:
        async with session_scope() as session:
            queue_result = await session.execute(
                update(QueueItem)
                .where(QueueItem.status.in_(("pending", "sending")))
                .values(status="skipped", last_error="skipped: bot was offline")
            )
            repeat_result = await session.execute(
                update(RepeatItem)
                .where(RepeatItem.status.in_(("pending", "sending")))
                .values(status="skipped", last_error="skipped: bot was offline")
            )
            post_result = await session.execute(
                update(Post)
                .where(Post.dispatched.is_(False))
                .values(dispatched=True, dispatched_at=utcnow())
            )
            return (
                int(queue_result.rowcount or 0)
                + int(repeat_result.rowcount or 0)
                + int(post_result.rowcount or 0)
            )

    async def requeue_stuck_items(self) -> int:
        """Return items left in 'sending' by a crash back to the pending pool."""
        async with session_scope() as session:
            result = await session.execute(
                update(QueueItem)
                .where(QueueItem.status == "sending")
                .values(status="pending", next_attempt_at=utcnow())
            )
            repeat_result = await session.execute(
                update(RepeatItem)
                .where(RepeatItem.status == "sending")
                .values(status="pending", next_attempt_at=utcnow())
            )
            return int(result.rowcount or 0) + int(repeat_result.rowcount or 0)
