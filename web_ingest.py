"""Channel polling for the web-account transport.

There is no push/webhook available to a browser session, so this polls
`messages.getHistory` on the source channel periodically. It fills the same
`posts` / `post_media` tables the HTTP ingestor (ingest.py) uses, so the
existing delay/dispatch/queue machinery in queue_worker.py needs no changes.
The one difference: message_id here is the channel's real, positive message
id, which is exactly what lets queue_worker choose the forward path over
reconstructing the post from scratch (see queue_worker._deliver).
"""

from __future__ import annotations

import asyncio
import datetime as dt
import logging
from typing import Any

from sqlalchemy import select

from config import Settings, get_settings, is_within_active_window
from db import Post, PostMedia, session_scope, settings_store, utcnow
from web_transport import SessionUnrecoverable, WebTransport

logger = logging.getLogger("eitaa.web_ingest")


def _channel_peer_id(settings: Settings) -> int:
    raw = str(settings.source_channel_id)
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(
            f"SOURCE_CHANNEL_ID must be the numeric peer id in web mode, got {raw!r}. "
            "Resolve it once (e.g. via messages.checkChatInvite or the dialog list) "
            "and put that number in .env."
        ) from exc


class WebIngestor:
    """Polls the source channel and mirrors new/edited posts into the DB."""

    def __init__(self, transport: WebTransport, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self.transport = transport
        self.peer_id = _channel_peer_id(self.settings)
        self._stop = asyncio.Event()

    def stop(self) -> None:
        self._stop.set()

    async def _sleep(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass

    async def skip_offline_history(self) -> None:
        latest = await self._fetch_history(min_id=0, limit=1)
        baseline = latest[0]["id"] if latest else 0
        await settings_store.set("web_last_message_id", baseline)
        logger.info("startup: skipping offline history, baselining at message_id=%s", baseline)

    async def run(self) -> None:
        # Baselining must succeed before polling starts: poll_once() would
        # otherwise resume from a stale web_last_message_id and re-ingest every
        # post published while the bot was down. A transient browser fault here
        # used to escape run() and kill the ingest task for the whole process
        # lifetime, leaving a bot that logged "bot started" and then silently
        # forwarded nothing -- so retry instead of dying.
        attempt = 0
        while not self._stop.is_set():
            try:
                await self.transport.ensure_session()
                await self.skip_offline_history()
                break
            except SessionUnrecoverable:
                raise
            except Exception:
                attempt += 1
                delay = min(60.0, 5.0 * attempt)
                logger.exception(
                    "startup baseline failed (attempt %d); retrying in %.0fs", attempt, delay
                )
                await self._sleep(delay)
        if self._stop.is_set():
            return

        while not self._stop.is_set():
            if not is_within_active_window(self.settings):
                await self._sleep(5.0)
                continue
            try:
                await self.transport.ensure_session()
                await self.poll_once()
            except SessionUnrecoverable:
                raise
            except Exception:
                logger.exception("web ingest poll failed")
            await self._sleep(self.settings.web_poll_interval_seconds)

    # ---------------------------------------------------------------- fetching

    async def _fetch_history(self, *, min_id: int, limit: int) -> list[dict[str, Any]]:
        driver = self.transport._require_driver()  # noqa: SLF001 - same module family

        def _fetch() -> Any:
            script = f"""
            return apiManager.invokeApi('messages.getHistory', {{
                peer: appPeersManager.getInputPeerById({self.peer_id}),
                offset_id: 0, offset_date: 0, add_offset: 0,
                limit: {int(limit)}, max_id: 0, min_id: {int(min_id)}, hash: 0
            }}).then(r => (r.messages || []).map(m => ({{
                id: m.id,
                date: m.date,
                edit_date: m.edit_date || null,
                grouped_id: m.grouped_id || null,
                message: m.message || null,
                has_media: !!m.media,
                media_type: m.media ? m.media._ : null,
            }}))).catch(e => ({{
                error: (e && (e.type || e.error_message || e.message)) || 'UNKNOWN',
                code: (e && (e.code || e.error_code)) || null,
            }}));
            """
            return driver.execute_script(script)

        result = await self.transport._run(_fetch, op="getHistory")  # noqa: SLF001
        if isinstance(result, dict) and "error" in result:
            # Telegram API errors are plain objects with `type`/`code` and no
            # `message`, so stringifying them yielded a useless "[object
            # Object]" that hid INVALID_LOGIN for days.
            code = result.get("code")
            suffix = f" (code {code})" if code else ""
            raise RuntimeError(f"getHistory failed: {result['error']}{suffix}")
        return list(result or [])

    # ----------------------------------------------------------------- polling

    async def poll_once(self) -> None:
        last_seen = await settings_store.get_int("web_last_message_id")
        messages = await self._fetch_history(min_id=last_seen, limit=self.settings.web_history_page_size)
        if not messages:
            return

        # getHistory returns newest-first; process oldest-first so grouped
        # (album) messages land in the DB in the order they were posted.
        messages = sorted(messages, key=lambda m: m["id"])
        new_max = last_seen

        groups: dict[Any, list[dict[str, Any]]] = {}
        singles: list[dict[str, Any]] = []
        for message in messages:
            if message["id"] <= last_seen:
                continue
            new_max = max(new_max, message["id"])
            grouped_id = message.get("grouped_id")
            if grouped_id:
                groups.setdefault(grouped_id, []).append(message)
            else:
                singles.append(message)

        for message in singles:
            await self._store_post([message])
        for grouped_id, items in groups.items():
            await self._store_post(sorted(items, key=lambda m: m["id"]), media_group_id=str(grouped_id))

        if new_max > last_seen:
            await settings_store.set("web_last_message_id", new_max)

        await self._recheck_pending_edits()

    async def _store_post(self, messages: list[dict[str, Any]], *, media_group_id: str | None = None) -> None:
        anchor = messages[0]
        text = next((m["message"] for m in messages if m.get("message")), None)
        has_media = any(m["has_media"] for m in messages)
        kind = "album" if len(messages) > 1 else ("media" if has_media else "text")
        if not text and not has_media:
            logger.info("skipping empty channel message %s", anchor["id"])
            return

        delay = await settings_store.get_int("delay_seconds")
        async with session_scope() as session:
            post = Post(
                source_chat_id=str(self.peer_id),
                message_id=anchor["id"],
                media_group_id=media_group_id,
                kind=kind,
                text=text,
                ready_at=utcnow() + dt.timedelta(seconds=max(delay, 0)),
            )
            session.add(post)
            await session.flush()
            for position, message in enumerate(messages):
                session.add(
                    PostMedia(
                        post_id=post.id,
                        position=position,
                        message_id=message["id"],
                        media_type=message.get("media_type") or "text",
                    )
                )
            post_id = post.id
        logger.info("captured channel post %s (kind=%s, parts=%d)", post_id, kind, len(messages))

    async def _recheck_pending_edits(self) -> None:
        """Re-fetch still-pending posts so a last-second edit ships correctly."""
        async with session_scope() as session:
            pending = (
                await session.execute(
                    select(Post).where(
                        Post.dispatched.is_(False), Post.source_chat_id == str(self.peer_id)
                    )
                )
            ).scalars().all()
            targets = [(p.id, p.message_id) for p in pending]
        if not targets:
            return

        min_id = min(mid for _, mid in targets) - 1
        max_id = max(mid for _, mid in targets)
        try:
            fresh = await self._fetch_history(
                min_id=min_id, limit=min(max_id - min_id + 1, 100)
            )
        except RuntimeError as exc:
            logger.warning("edit recheck failed: %s", exc)
            return
        by_id = {m["id"]: m for m in fresh}

        async with session_scope() as session:
            for post_id, message_id in targets:
                fresh_message = by_id.get(message_id)
                if fresh_message is None or not fresh_message.get("edit_date"):
                    continue
                post = await session.get(Post, post_id)
                if post is None or post.dispatched:
                    continue
                new_text = fresh_message.get("message")
                if new_text != post.text:
                    post.text = new_text
                    post.edited_at = utcnow()
                    logger.info("post %s updated from channel edit", post_id)
