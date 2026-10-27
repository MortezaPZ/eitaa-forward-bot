"""Keeps groups active during a quiet channel by re-forwarding recent posts.

Client's spec: if the channel goes 1 hour without a new post, forward the 10
most recent real posts to every group. If it's still quiet an hour later,
forward the 10 before those (posts 11-20 back), and so on — one batch further
back into history per additional idle hour — until a genuinely new post
appears, which stops the cycle immediately (idle time resets to ~0).

Only makes sense for the web/account transport: forwarding a real message by
id requires the account to actually be a member and able to see it, which is
exactly what web_ingest.py already captures via `Post.message_id` (positive,
real channel message ids) and `Post.source_chat_id`. The eitaayar/bot API has
no forward endpoint at all (see HANDOFF.md), so this stays inert there.

Dispatch only creates rows in `repeat_items` (see db.py) — actually sending
them is queue_worker.QueueWorker.repeat_sender_loop, which shares the same
rate limiter as normal delivery so total outbound traffic stays capped.
"""

from __future__ import annotations

import asyncio
import logging

from sqlalchemy import select

from config import Settings, get_settings, is_within_active_window
from db import Group, Post, RepeatItem, session_scope, settings_store, utcnow

logger = logging.getLogger("eitaa.repeater")


class Repeater:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self._stop = asyncio.Event()
        # Idle time is capped at how long THIS process has been continuously
        # watching the channel — never at how stale the DB's last post looks.
        # Without this, restarting the bot after any downtime (or racing
        # ahead of the ingest catch-up on startup) misreads "we were offline"
        # as "the channel went quiet" and fires every missed round at once.
        self._started_at = utcnow()

    def stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        if not self.settings.repeat_enabled:
            logger.info("repeat feature is disabled")
            return
        if self.settings.transport != "web":
            logger.warning("repeat feature needs TRANSPORT=web (forwarding); not starting")
            return
        while not self._stop.is_set():
            try:
                await self.tick()
            except Exception:
                logger.exception("repeater tick failed")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.settings.repeat_poll_seconds)
            except asyncio.TimeoutError:
                continue

    def _within_active_window(self) -> bool:
        return is_within_active_window(self.settings)

    async def tick(self) -> None:
        if not self._within_active_window():
            return

        idle_seconds = self.settings.repeat_idle_hours * 3600
        n = self.settings.repeat_last_n_posts

        async with session_scope() as session:
            latest = (
                await session.execute(
                    select(Post)
                    .where(Post.message_id > 0)  # real channel posts only, not manage.py publish
                    .order_by(Post.id.desc())
                    .limit(1)
                )
            ).scalar_one_or_none()
            if latest is None:
                return
            anchor_id = latest.id
            elapsed_since_post = (utcnow() - latest.created_at).total_seconds()

        monitored_seconds = (utcnow() - self._started_at).total_seconds()
        elapsed = min(elapsed_since_post, monitored_seconds)

        required_rounds = int(elapsed // idle_seconds)
        if required_rounds <= 0:
            return

        stored_anchor = await settings_store.get_int("repeat_anchor_post_id")
        rounds_done = await settings_store.get_int("repeat_rounds_done") if stored_anchor == anchor_id else 0

        if required_rounds <= rounds_done:
            return

        groups_created_for: set[int] | None = None
        for round_index in range(rounds_done, required_rounds):
            created = await self._dispatch_round(anchor_id, round_index, n)
            if not created:
                # Ran out of history to repeat; stop advancing further rounds
                # until a genuinely new post moves the anchor.
                break
            rounds_done = round_index + 1

        await settings_store.set("repeat_anchor_post_id", anchor_id)
        await settings_store.set("repeat_rounds_done", rounds_done)

    async def _dispatch_round(self, anchor_id: int, round_index: int, n: int) -> int:
        async with session_scope() as session:
            batch = (
                await session.execute(
                    select(Post)
                    .where(Post.message_id > 0)
                    .order_by(Post.id.desc())
                    .offset(round_index * n)
                    .limit(n)
                )
            ).scalars().all()
            if not batch:
                return 0

            groups = (
                await session.execute(
                    select(Group).where(Group.enabled.is_(True), Group.status != "kicked")
                )
            ).scalars().all()

            created = 0
            for post in batch:
                for group in groups:
                    exists = (
                        await session.execute(
                            select(RepeatItem.id).where(
                                RepeatItem.post_id == post.id,
                                RepeatItem.group_id == group.id,
                                RepeatItem.round == round_index,
                            )
                        )
                    ).scalar_one_or_none()
                    if exists is not None:
                        continue
                    session.add(RepeatItem(post_id=post.id, group_id=group.id, round=round_index))
                    created += 1

            logger.info(
                "idle repeat round %d (anchor=%s): queued %d items for %d posts x %d groups",
                round_index, anchor_id, created, len(batch), len(groups),
            )
            return created
