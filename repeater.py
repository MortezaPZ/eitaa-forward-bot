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

from typing import Any

from sqlalchemy import select

from config import Settings, get_settings, is_within_active_window
from db import Group, Post, RepeatItem, session_scope, settings_store, utcnow
from queue_worker import BUNDLE_ROUND_OFFSET

logger = logging.getLogger("eitaa.repeater")

# A run's rounds are stored as cycle * ROUND_CYCLE_STRIDE + round_index, which
# keeps every run's rows distinct under the (post_id, group_id, round) unique
# constraint while staying an ordinary small integer well below
# BUNDLE_ROUND_OFFSET, where queue_worker's context-bundle rounds begin.
ROUND_CYCLE_STRIDE = 1000
MAX_CYCLE = BUNDLE_ROUND_OFFSET // ROUND_CYCLE_STRIDE


class Repeater:
    def __init__(
        self,
        settings: Settings | None = None,
        transport: Any = None,
        worker: Any = None,
    ) -> None:
        self.settings = settings or get_settings()
        # Batches are named by channel message id, so the repeater needs the
        # channel reader and the worker's stub-post helper.
        self.transport = transport
        self.worker = worker
        self._stop = asyncio.Event()
        self._cycle = 0
        self._anchor_seen_id: int | None = None
        self._anchor_seen_at = None
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

        # Every startup begins a fresh idle cycle. Without this the counter
        # survives downtime: an anchor post that already had N rounds sent
        # keeps its rounds_done, so after a restart the repeater sits idle
        # until it has watched the channel for more than N hours -- which,
        # for an anchor left over from days ago, means it effectively never
        # runs again until a new post arrives. Resetting pairs with the
        # _started_at cap below: the first round still needs a full
        # REPEAT_IDLE_HOURS of continuous, observed silence.
        #
        # Resetting the counter alone is not enough. repeat_items is unique on
        # (post_id, group_id, round), so rows written by earlier runs of round
        # N block this run from queueing round N again -- a post already
        # repeated to a group once could never be repeated to it again. Each
        # run therefore gets its own cycle number, folded into the stored
        # round so this run's rows cannot collide with any previous run's.
        cycle = await settings_store.get_int("repeat_cycle") + 1
        if cycle >= MAX_CYCLE:  # keep cycle*STRIDE clear of BUNDLE_ROUND_OFFSET
            cycle = 1
        self._cycle = cycle
        await settings_store.set("repeat_cycle", cycle)
        await settings_store.set("repeat_rounds_done", 0)
        await settings_store.set("repeat_anchor_post_id", 0)
        logger.info("idle-repeat cycle reset for this run (cycle=%d)", cycle)

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
        if self.transport is None or self.worker is None:
            return

        idle_seconds = self.settings.repeat_idle_hours * 3600
        n = self.settings.repeat_last_n_posts

        # "Quiet" means the channel has published nothing, so the anchor is the
        # channel's newest message. Reading it from `posts` measured only what
        # the bot happened to ingest, which after any downtime is a different
        # question entirely.
        try:
            newest = await self.transport.get_channel_message_ids(
                str(self.settings.source_channel_id), limit=1
            )
        except Exception as exc:
            logger.warning("could not read the channel to check for silence: %s", exc)
            return
        if not newest:
            return
        anchor_id = newest[0]

        if self._anchor_seen_id != anchor_id:
            # A new post: the channel is not quiet, so the cycle restarts.
            self._anchor_seen_id = anchor_id
            self._anchor_seen_at = utcnow()
            await settings_store.set("repeat_anchor_post_id", anchor_id)
            await settings_store.set("repeat_rounds_done", 0)
            return

        # Idle time is capped by how long THIS process has been watching, so a
        # restart never treats past downtime as observed silence.
        quiet_for = (utcnow() - (self._anchor_seen_at or self._started_at)).total_seconds()
        monitored = (utcnow() - self._started_at).total_seconds()
        elapsed = min(quiet_for, monitored)

        required_rounds = int(elapsed // idle_seconds)
        if required_rounds <= 0:
            return

        rounds_done = await settings_store.get_int("repeat_rounds_done")
        if required_rounds <= rounds_done:
            return

        for round_index in range(rounds_done, required_rounds):
            created = await self._dispatch_round(anchor_id, round_index, n)
            if not created:
                break
            rounds_done = round_index + 1

        await settings_store.set("repeat_anchor_post_id", anchor_id)
        await settings_store.set("repeat_rounds_done", rounds_done)

    def _round_key(self, round_index: int) -> int:
        """The value stored in repeat_items.round for this run's round N."""
        return self._cycle * ROUND_CYCLE_STRIDE + round_index

    async def _dispatch_round(self, anchor_id: int, round_index: int, n: int) -> int:
        if round_index >= ROUND_CYCLE_STRIDE:
            # ~1000 idle hours in one uninterrupted run; the batch offset would
            # have run past the end of the post history long before this.
            logger.warning("round %d exceeds the per-cycle range; not dispatching", round_index)
            return 0
        round_key = self._round_key(round_index)
        # Round 0 is the channel's newest n messages, round 1 the n before
        # those, and so on -- counted in the channel, not in `posts`.
        try:
            ids = await self.transport.get_channel_message_ids(
                str(self.settings.source_channel_id),
                skip=round_index * n,
                limit=n,
            )
        except Exception as exc:
            logger.warning("could not read the channel for round %d: %s", round_index, exc)
            return 0
        if not ids:
            return 0

        async with session_scope() as session:
            batch = []
            for message_id in ids:
                row = await self.worker.ensure_channel_post(session, message_id)
                if row is not None:
                    batch.append(row)
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
                                RepeatItem.round == round_key,
                            )
                        )
                    ).scalar_one_or_none()
                    if exists is not None:
                        continue
                    session.add(RepeatItem(post_id=post.id, group_id=group.id, round=round_key))
                    created += 1

            logger.info(
                "idle repeat round %d of cycle %d (anchor=%s): "
                "queued %d items for %d posts x %d groups",
                round_index, self._cycle, anchor_id, created, len(batch), len(groups),
            )
            return created
