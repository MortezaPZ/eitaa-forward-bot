from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
from pathlib import Path
from typing import Any

from sqlalchemy import select

from config import Settings, get_settings
from db import Post, PostMedia, session_scope, settings_store, utcnow
from eitaa_client import EitaaClient, EitaaError, RateLimitError

logger = logging.getLogger("eitaa.ingest")

MEDIA_FIELDS: tuple[str, ...] = (
    "photo",
    "video",
    "audio",
    "voice",
    "document",
    "animation",
    "video_note",
    "sticker",
)


def extract_media(message: dict[str, Any]) -> dict[str, Any] | None:
    """Return a normalised descriptor for the single attachment of a message."""
    for field in MEDIA_FIELDS:
        value = message.get(field)
        if not value:
            continue
        if field == "photo" and isinstance(value, list):
            value = max(value, key=lambda item: item.get("file_size") or 0)
        if not isinstance(value, dict):
            continue
        return {
            "media_type": field,
            "file_id": value.get("file_id"),
            "file_name": value.get("file_name"),
            "mime_type": value.get("mime_type"),
        }
    return None


def message_text(message: dict[str, Any]) -> str | None:
    return message.get("text") or message.get("caption")


class Ingestor:
    """Polls the source channel and turns updates into persisted posts."""

    def __init__(self, client: EitaaClient, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self.client = client
        self._albums: dict[str, dict[str, Any]] = {}
        self._album_lock = asyncio.Lock()
        self._command_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._stop = asyncio.Event()

    @property
    def command_queue(self) -> "asyncio.Queue[dict[str, Any]]":
        return self._command_queue

    def stop(self) -> None:
        self._stop.set()

    # ------------------------------------------------------------------ polling

    async def run(self) -> None:
        if not self.client.is_botapi:
            logger.warning(
                "api_mode=eitaayar has no getUpdates support; channel ingestion is disabled"
            )
            return

        offset = await settings_store.get_int("update_offset")
        backoff = 1.0
        while not self._stop.is_set():
            try:
                updates = await self.client.get_updates(offset)
                backoff = 1.0
            except RateLimitError as exc:
                logger.warning("getUpdates rate limited, sleeping %.1fs", exc.retry_after)
                await self._sleep(exc.retry_after)
                continue
            except EitaaError as exc:
                logger.error("getUpdates failed: %s", exc.message)
                await self._sleep(backoff)
                backoff = min(backoff * 2, 60.0)
                continue
            except Exception:
                logger.exception("unexpected error while polling updates")
                await self._sleep(backoff)
                backoff = min(backoff * 2, 60.0)
                continue

            for update in updates:
                update_id = int(update.get("update_id", 0))
                offset = max(offset, update_id + 1)
                try:
                    await self.handle_update(update)
                except Exception:
                    logger.exception("failed to handle update %s", update_id)

            if updates:
                await settings_store.set("update_offset", offset)
            else:
                await self._sleep(0.5)

    async def _sleep(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass

    # ------------------------------------------------------------------ routing

    async def handle_update(self, update: dict[str, Any]) -> None:
        for key in ("channel_post", "edited_channel_post", "message", "edited_message"):
            message = update.get(key)
            if not isinstance(message, dict):
                continue
            chat = message.get("chat") or {}
            chat_id = str(chat.get("id") or "")
            username = chat.get("username")
            is_source = chat_id == str(self.settings.source_channel_id) or (
                username and f"@{username}" == str(self.settings.source_channel_id)
            )
            if is_source:
                await self.handle_channel_message(message, edited=key.startswith("edited"))
            elif chat.get("type") == "private":
                await self._command_queue.put(message)
            return

    # ------------------------------------------------------------ channel posts

    async def handle_channel_message(self, message: dict[str, Any], *, edited: bool) -> None:
        media_group_id = message.get("media_group_id")
        if media_group_id:
            await self._buffer_album(str(media_group_id), message, edited=edited)
            return
        await self._store_post([message], edited=edited)

    async def _buffer_album(
        self, media_group_id: str, message: dict[str, Any], *, edited: bool
    ) -> None:
        async with self._album_lock:
            bucket = self._albums.get(media_group_id)
            if bucket is None:
                bucket = {"messages": [], "task": None, "edited": edited}
                self._albums[media_group_id] = bucket
            bucket["messages"].append(message)
            bucket["edited"] = bucket["edited"] or edited
            task: asyncio.Task[None] | None = bucket.get("task")
            if task is not None:
                task.cancel()
            bucket["task"] = asyncio.create_task(self._flush_album_later(media_group_id))

    async def _flush_album_later(self, media_group_id: str) -> None:
        try:
            await asyncio.sleep(self.settings.media_group_window_seconds)
        except asyncio.CancelledError:
            return
        async with self._album_lock:
            bucket = self._albums.pop(media_group_id, None)
        if not bucket:
            return
        messages = sorted(bucket["messages"], key=lambda item: item.get("message_id", 0))
        try:
            await self._store_post(messages, edited=bool(bucket["edited"]))
        except Exception:
            logger.exception("failed to store album %s", media_group_id)

    # ------------------------------------------------------------------ storage

    async def _store_post(self, messages: list[dict[str, Any]], *, edited: bool) -> None:
        """Persist (or update) a post. The anchor message_id gives idempotency."""
        if not messages:
            return
        anchor = messages[0]
        source_chat_id = str((anchor.get("chat") or {}).get("id") or self.settings.source_channel_id)
        message_id = int(anchor.get("message_id", 0))
        media_group_id = anchor.get("media_group_id")

        media_items: list[dict[str, Any]] = []
        for message in messages:
            descriptor = extract_media(message)
            if descriptor is None:
                continue
            descriptor["caption"] = message.get("caption")
            descriptor["message_id"] = message.get("message_id")
            media_items.append(descriptor)

        text = None
        for message in messages:
            text = message_text(message)
            if text:
                break

        if not media_items and not text:
            logger.info("skipping unsupported post %s (no text, no media)", message_id)
            return

        if media_items:
            kind = "album" if len(media_items) > 1 else "media"
        else:
            kind = "text"

        delay = await settings_store.get_int("delay_seconds")
        ready_at = utcnow() + dt.timedelta(seconds=max(delay, 0))

        async with session_scope() as session:
            existing = (
                await session.execute(
                    select(Post).where(
                        Post.source_chat_id == source_chat_id, Post.message_id == message_id
                    )
                )
            ).scalar_one_or_none()

            if existing is None and media_group_id:
                existing = (
                    await session.execute(
                        select(Post).where(Post.media_group_id == str(media_group_id))
                    )
                ).scalar_one_or_none()

            if existing is not None:
                if existing.dispatched:
                    logger.info(
                        "post %s already dispatched; ignoring %s",
                        existing.id,
                        "edit" if edited else "duplicate",
                    )
                    return
                existing.text = text
                existing.kind = kind
                existing.edited_at = utcnow()
                existing.raw = json.dumps(messages, ensure_ascii=False)[:200000]
                for row in list(existing.media):
                    await session.delete(row)
                await session.flush()
                post = existing
                logger.info("updated pending post %s from edit", post.id)
            else:
                post = Post(
                    source_chat_id=source_chat_id,
                    message_id=message_id,
                    media_group_id=str(media_group_id) if media_group_id else None,
                    kind=kind,
                    text=text,
                    raw=json.dumps(messages, ensure_ascii=False)[:200000],
                    ready_at=ready_at,
                )
                session.add(post)
                await session.flush()
                logger.info("captured post %s (kind=%s, media=%d)", post.id, kind, len(media_items))

            for position, item in enumerate(media_items):
                session.add(
                    PostMedia(
                        post_id=post.id,
                        position=position,
                        message_id=item.get("message_id"),
                        media_type=item["media_type"],
                        file_id=item.get("file_id"),
                        file_name=item.get("file_name"),
                        mime_type=item.get("mime_type"),
                        caption=item.get("caption"),
                    )
                )
            post_id = post.id

        await self._cache_media(post_id)

    async def _cache_media(self, post_id: int) -> None:
        """Download attachments once so retries never depend on a live file_id."""
        async with session_scope() as session:
            rows = (
                await session.execute(select(PostMedia).where(PostMedia.post_id == post_id))
            ).scalars().all()
            targets = [(row.id, row.file_id, row.file_name, row.media_type) for row in rows]

        for row_id, file_id, file_name, media_type in targets:
            if not file_id:
                continue
            suffix = Path(file_name).suffix if file_name else ""
            destination = self.settings.files_dir / f"{post_id}_{row_id}{suffix}"
            saved = await self.client.download_file(file_id, destination)
            if saved is None:
                continue
            async with session_scope() as session:
                row = await session.get(PostMedia, row_id)
                if row is not None:
                    row.local_path = str(saved)
            logger.debug("cached %s for post %s", media_type, post_id)
