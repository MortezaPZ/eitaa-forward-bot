from __future__ import annotations

import datetime as dt
import asyncio
from typing import Any, Sequence

import pytest
from sqlalchemy import delete, select

from commands import CommandRouter, add_managed_group, normalize_chat_id
from db import (
    Group,
    Post,
    PostMedia,
    QueueItem,
    RepeatItem,
    SendLog,
    init_db,
    session_scope,
    settings_store,
    utcnow,
)
from eitaa_client import EitaaError
from ingest import Ingestor
from queue_worker import BUNDLE_ROUND_OFFSET, QueueWorker


class FakeTransport:
    is_botapi = True

    def __init__(self, channel_ids: list[int] | None = None) -> None:
        self.sent: list[tuple[str, Any]] = []
        self.groups: list[tuple[str, Sequence[dict[str, Any]]]] = []
        # Stands in for the real channel history. Newest first, exactly as
        # messages.getHistory returns it.
        self.channel_ids = channel_ids if channel_ids is not None else []
        self.history_calls: list[dict[str, Any]] = []

    async def get_channel_message_ids(
        self, peer_id: str, *, before_id: int | None = None, skip: int = 0, limit: int = 10
    ) -> list[int]:
        self.history_calls.append({"before_id": before_id, "skip": skip, "limit": limit})
        ids = list(self.channel_ids)
        if before_id:
            ids = [i for i in ids if i < before_id]
        return ids[skip : skip + limit]

    async def send_message(self, chat_id: str, text: str, **kwargs: Any) -> Any:
        self.sent.append(("text", chat_id, text))
        return {"ok": True}

    async def send_media(self, chat_id: str, media_type: str, **kwargs: Any) -> Any:
        self.sent.append(("media", chat_id, media_type))
        return {"ok": True}

    async def send_media_group(self, chat_id: str, items: Sequence[dict[str, Any]], **kwargs: Any) -> Any:
        self.groups.append((chat_id, list(items)))
        return {"ok": True}

    async def download_file(self, file_id: str, destination: Any) -> None:
        return None

    async def aclose(self) -> None:
        return None


@pytest.fixture
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


async def _prepare() -> None:
    await init_db()
    await settings_store.load()
    async with session_scope() as session:
        await session.execute(delete(SendLog))
        await session.execute(delete(QueueItem))
        await session.execute(delete(RepeatItem))
        await session.execute(delete(PostMedia))
        await session.execute(delete(Post))
        await session.execute(delete(Group))
    await settings_store.set("delay_seconds", 0)
    await settings_store.set("paused", False)
    await settings_store.set("send_interval", 0.01)
    await settings_store.set("max_per_minute", 600)


def test_normalize_numeric():
    assert normalize_chat_id("-100123") == "-100123"


def test_normalize_link():
    assert normalize_chat_id("https://eitaa.com/mygroup") == "@mygroup"


def test_normalize_username():
    assert normalize_chat_id("@FooBar") == "@FooBar"


def test_normalize_joinchat():
    assert normalize_chat_id("https://eitaa.com/joinchat/AbC_12") == "https://eitaa.com/joinchat/AbC_12"


def _open_active_window(settings) -> None:
    """Let the repeater act whatever the clock says.

    Repeater.tick() does nothing outside ACTIVE_START_HOUR..ACTIVE_END_HOUR,
    which silently turned these tests into pass-before-midnight-only: they
    started failing at 00:00 with no code change behind it.
    """
    settings.active_start_hour = 0
    settings.active_end_hour = 24
    settings.repeat_active_start_hour = 0
    settings.repeat_active_end_hour = 24



@pytest.mark.asyncio
async def test_add_group_and_duplicate():
    await _prepare()
    code, msg = await add_managed_group("-1001", "one")
    assert code == 0 and "added" in msg
    code, msg = await add_managed_group("-1001", "one")
    assert code == 0 and "already existed" in msg
    async with session_scope() as session:
        n = len((await session.execute(select(Group))).scalars().all())
    assert n == 1


@pytest.mark.asyncio
async def test_non_admin_command_rejected():
    await _prepare()
    fake = FakeTransport()
    worker = QueueWorker(fake)  # type: ignore[arg-type]
    router = CommandRouter(fake, worker, asyncio.Queue())  # type: ignore[arg-type]
    await router.handle_message({"text": "/list", "from": {"id": 99}, "chat": {"id": 99}})
    assert fake.sent == []


def _photo(mid: int, group: str | None = None) -> dict:
    msg = {
        "message_id": mid,
        "chat": {"id": -100111},
        "photo": [{"file_id": f"f{mid}", "file_size": 10}],
    }
    if group:
        msg["media_group_id"] = group
    return msg


@pytest.mark.asyncio
async def test_edit_merges_pending_post():
    await _prepare()
    ingestor = Ingestor(FakeTransport())  # type: ignore[arg-type]
    await ingestor._store_post([{"message_id": 5, "chat": {"id": -100111}, "text": "old"}], edited=False)
    await ingestor._store_post([{"message_id": 5, "chat": {"id": -100111}, "text": "new"}], edited=True)
    async with session_scope() as session:
        posts = (await session.execute(select(Post))).scalars().all()
        assert len(posts) == 1
        assert posts[0].text == "new"


@pytest.mark.asyncio
async def test_album_stored_as_one_post_ordered():
    await _prepare()
    ingestor = Ingestor(FakeTransport())  # type: ignore[arg-type]
    await ingestor._store_post([_photo(10, "g1"), _photo(11, "g1"), _photo(12, "g1")], edited=False)
    async with session_scope() as session:
        posts = (await session.execute(select(Post))).scalars().all()
        assert len(posts) == 1
        assert posts[0].kind == "album"
        media = (
            await session.execute(select(PostMedia).where(PostMedia.post_id == posts[0].id).order_by(PostMedia.position))
        ).scalars().all()
        assert [m.message_id for m in media] == [10, 11, 12]


@pytest.mark.asyncio
async def test_dispatch_fanout_and_idempotency():
    await _prepare()
    async with session_scope() as session:
        session.add(Group(chat_id="-2001"))
        session.add(Group(chat_id="-2002"))
        session.add(Post(source_chat_id="-100111", message_id=1, kind="text", text="hi", ready_at=utcnow()))
    worker = QueueWorker(FakeTransport())  # type: ignore[arg-type]
    created = await worker.dispatch_due_posts()
    assert created == 2
    created_again = await worker.dispatch_due_posts()
    assert created_again == 0


@pytest.mark.asyncio
async def test_kicked_group_disabled():
    await _prepare()
    async with session_scope() as session:
        session.add(Group(chat_id="-3001"))
        session.add(Post(source_chat_id="-100111", message_id=2, kind="text", text="x", ready_at=utcnow()))
    worker = QueueWorker(FakeTransport())  # type: ignore[arg-type]
    worker.settings.bundle_enabled = False  # exercise the plain queue path
    await worker.dispatch_due_posts()

    class KickTransport(FakeTransport):
        is_botapi = False

        async def send_message(self, chat_id: str, text: str, **kwargs: Any) -> Any:
            raise EitaaError("bot was kicked", permanent=True)

    worker.client = KickTransport()  # type: ignore[assignment]
    item = await worker._claim_next_item()
    assert item is not None
    await worker._deliver(item)
    async with session_scope() as session:
        group = (await session.execute(select(Group))).scalar_one()
        assert group.enabled is False
        assert group.status == "kicked"


@pytest.mark.asyncio
async def test_kicked_surfaces_in_list_status():
    await _prepare()
    from commands import CommandRouter

    async with session_scope() as session:
        session.add(Group(chat_id="-4001", enabled=False, status="kicked"))
    fake = FakeTransport()
    router = CommandRouter(fake, QueueWorker(fake), asyncio.Queue())  # type: ignore[arg-type]
    listing = await router.cmd_list({}, "")
    status = await router.cmd_status({}, "")
    assert "kicked" in listing
    assert "گروه‌های فعال: 0" in status


@pytest.mark.asyncio
async def test_album_sent_as_single_unit_when_backend_supports():
    await _prepare()
    fake = FakeTransport()
    worker = QueueWorker(fake)  # type: ignore[arg-type]
    await worker._send_payload(
        "-1",
        "album",
        None,
        [
            {"media_type": "photo", "file_id": "a"},
            {"media_type": "photo", "file_id": "b"},
        ],
    )
    assert len(fake.groups) == 1
    assert fake.sent == []


@pytest.mark.asyncio
async def test_new_post_cancels_pending_idle_repeats_not_bundle():
    await _prepare()
    # Message 10 precedes the new message 20 in the channel.
    worker = QueueWorker(FakeTransport(channel_ids=[20, 10]))  # type: ignore[arg-type]
    previous_bundle = worker.settings.bundle_enabled
    previous_count = worker.settings.bundle_previous_count
    worker.settings.bundle_enabled = True
    worker.settings.bundle_previous_count = 9
    try:
        async with session_scope() as session:
            group = Group(chat_id="-5001")
            session.add(group)
            await session.flush()
            predecessor = Post(
                source_chat_id="-100111",
                message_id=10,
                kind="text",
                text="old",
                ready_at=utcnow(),
                dispatched=True,
            )
            session.add(predecessor)
            await session.flush()
            session.add(
                RepeatItem(
                    post_id=predecessor.id,
                    group_id=group.id,
                    round=0,
                    status="pending",
                )
            )
            session.add(
                Post(
                    source_chat_id="-100111",
                    message_id=20,
                    kind="text",
                    text="new",
                    ready_at=utcnow(),
                    dispatched=False,
                )
            )
        await worker.dispatch_due_posts()
        async with session_scope() as session:
            idle = (
                await session.execute(select(RepeatItem).where(RepeatItem.round < BUNDLE_ROUND_OFFSET))
            ).scalars().all()
            bundle = (
                await session.execute(select(RepeatItem).where(RepeatItem.round >= BUNDLE_ROUND_OFFSET))
            ).scalars().all()
        assert idle
        assert all(item.status == "skipped" and "cancelled" in (item.last_error or "") for item in idle)
        assert bundle
        assert all(item.status == "pending" for item in bundle)
    finally:
        worker.settings.bundle_enabled = previous_bundle
        worker.settings.bundle_previous_count = previous_count


@pytest.mark.asyncio
async def test_sending_idle_repeat_not_cancelled_by_new_post():
    await _prepare()
    worker = QueueWorker(FakeTransport())  # type: ignore[arg-type]
    async with session_scope() as session:
        group = Group(chat_id="-6001")
        session.add(group)
        await session.flush()
        old = Post(
            source_chat_id="-100111",
            message_id=30,
            kind="text",
            text="old",
            ready_at=utcnow(),
            dispatched=True,
        )
        session.add(old)
        await session.flush()
        session.add(
            RepeatItem(post_id=old.id, group_id=group.id, round=0, status="sending")
        )
        session.add(
            Post(
                source_chat_id="-100111",
                message_id=31,
                kind="text",
                text="new",
                ready_at=utcnow(),
                dispatched=False,
            )
        )
    await worker.dispatch_due_posts()
    async with session_scope() as session:
        rows = (await session.execute(select(RepeatItem).where(RepeatItem.round == 0))).scalars().all()
    assert len(rows) == 1
    assert rows[0].status == "sending"
    assert rows[0].last_error is None


@pytest.mark.asyncio
async def test_browser_run_reapplies_script_timeout():
    from unittest.mock import MagicMock

    from web_transport import WebTransport

    transport = WebTransport()
    transport._driver = MagicMock()
    result = await transport._run(lambda: 7, op="forward", chat_id="-1")
    assert result == 7
    transport._driver.set_script_timeout.assert_called_with(60)


@pytest.mark.asyncio
async def test_repeater_resets_round_counter_on_every_start():
    """A restart must begin a fresh idle cycle.

    The counter used to persist, so an anchor that had already had N rounds
    sent stayed at N: after a restart the repeater refused to act until it
    had watched the channel for more than N hours, which for a stale anchor
    meant it never repeated again until a brand-new post arrived.
    """
    from repeater import Repeater

    await _prepare()
    await settings_store.set("repeat_rounds_done", 2)
    await settings_store.set("repeat_anchor_post_id", 80)

    repeater = Repeater()
    repeater.settings.repeat_enabled = True
    repeater.settings.transport = "web"
    # Let run() do its startup work, then stop it before the first sleep ends.
    task = asyncio.create_task(repeater.run())
    await asyncio.sleep(0.2)
    repeater.stop()
    await asyncio.wait_for(task, timeout=5)

    assert await settings_store.get_int("repeat_rounds_done") == 0
    assert await settings_store.get_int("repeat_anchor_post_id") == 0


@pytest.mark.asyncio
async def test_repeater_still_waits_a_full_idle_window_after_reset():
    """Resetting the counter must not let a restart fire rounds immediately.

    The reset pairs with the _started_at cap: a bot that just came up has
    observed no silence yet, so nothing may go out until it has actually
    watched the channel stay quiet for REPEAT_IDLE_HOURS.
    """
    from repeater import Repeater

    await _prepare()
    async with session_scope() as session:
        session.add(Group(chat_id="-5001"))
        session.add(
            Post(
                source_chat_id="-100111",
                message_id=500,
                kind="text",
                text="old post",
                ready_at=utcnow(),
                dispatched=True,
                created_at=utcnow() - dt.timedelta(days=5),
            )
        )

    fake = FakeTransport(channel_ids=[500])
    worker = QueueWorker(fake)  # type: ignore[arg-type]
    repeater = Repeater(transport=fake, worker=worker)
    repeater.settings.repeat_enabled = True
    repeater.settings.repeat_idle_hours = 1
    _open_active_window(repeater.settings)
    await settings_store.set("repeat_rounds_done", 0)
    await settings_store.set("repeat_anchor_post_id", 0)

    # Just started: the post is 5 days old, but nothing has been observed yet.
    await repeater.tick()
    assert repeater._anchor_seen_id == 500, "the first tick records the anchor"
    await repeater.tick()
    async with session_scope() as session:
        queued = (await session.execute(select(RepeatItem))).scalars().all()
    assert queued == [], "a fresh start must not fire rounds for pre-existing silence"

    # After an hour of observed silence, the first round goes out.
    repeater._started_at = utcnow() - dt.timedelta(hours=1, minutes=1)
    repeater._anchor_seen_at = utcnow() - dt.timedelta(hours=1, minutes=1)
    await repeater.tick()
    async with session_scope() as session:
        queued = (await session.execute(select(RepeatItem))).scalars().all()
    assert len(queued) == 1
    assert queued[0].round == 0


@pytest.mark.asyncio
async def test_new_cycle_repeats_posts_an_earlier_run_already_sent():
    """The regression that made the counter reset useless in practice.

    repeat_items is unique on (post_id, group_id, round). A previous run's
    round-0 rows therefore blocked a later run from queueing round 0 again:
    live, a reset cycle produced 10 rows instead of 590 because every other
    (post, group) pair had already been repeated days earlier. Each run now
    stores its rounds under its own cycle, so old rows cannot suppress it.
    """
    from repeater import ROUND_CYCLE_STRIDE, Repeater

    await _prepare()
    async with session_scope() as session:
        group = Group(chat_id="-5001")
        session.add(group)
        await session.flush()
        post = Post(
            source_chat_id="-100111",
            message_id=700,
            kind="text",
            text="already repeated once",
            ready_at=utcnow(),
            dispatched=True,
            created_at=utcnow() - dt.timedelta(days=5),
        )
        session.add(post)
        await session.flush()
        # Exactly what an earlier run left behind.
        session.add(RepeatItem(post_id=post.id, group_id=group.id, round=0, status="done"))
        post_id, group_id = post.id, group.id

    fake = FakeTransport(channel_ids=[700])
    worker = QueueWorker(fake)  # type: ignore[arg-type]
    repeater = Repeater(transport=fake, worker=worker)
    repeater.settings.repeat_enabled = True
    repeater.settings.repeat_idle_hours = 1
    _open_active_window(repeater.settings)
    # What run() does at startup, minus the browser it would need.
    repeater._cycle = 1
    await settings_store.set("repeat_rounds_done", 0)
    await settings_store.set("repeat_anchor_post_id", 0)
    repeater._started_at = utcnow() - dt.timedelta(hours=1, minutes=1)

    await repeater.tick()  # records the anchor
    repeater._anchor_seen_at = utcnow() - dt.timedelta(hours=1, minutes=1)
    await repeater.tick()  # now an hour of observed silence has passed

    async with session_scope() as session:
        rows = (
            await session.execute(
                select(RepeatItem).where(RepeatItem.group_id == group_id)
            )
        ).scalars().all()
    rounds = sorted(r.round for r in rows)
    assert rounds == [0, ROUND_CYCLE_STRIDE], (
        "the new cycle must queue its own row instead of being blocked by the old one"
    )
    fresh = [r for r in rows if r.round == ROUND_CYCLE_STRIDE]
    assert fresh[0].status == "pending"


@pytest.mark.asyncio
async def test_idle_repeat_rounds_stay_clear_of_bundle_rounds():
    """Cycle-scoped rounds must not stray into the context-bundle range.

    _cancel_pending_idle_repeats tells the two mechanisms apart purely by
    `round < BUNDLE_ROUND_OFFSET`, so a cycle number large enough to cross
    that line would make a new post stop cancelling idle repeats.
    """
    from repeater import MAX_CYCLE, ROUND_CYCLE_STRIDE, Repeater

    repeater = Repeater()
    repeater._cycle = MAX_CYCLE - 1
    assert repeater._round_key(ROUND_CYCLE_STRIDE - 1) < BUNDLE_ROUND_OFFSET


def test_numeric_peer_id_rejects_usernames_permanently():
    """@username groups must fail cleanly, not as an "unexpected" fault.

    The web forward path resolves peers by numeric id. A group stored as
    '@testgroup1' used to reach int() and raise a bare ValueError, which the
    queue logged as unexpected and retried with backoff forever instead of
    disabling the group.
    """
    from web_transport import _numeric_peer_id

    assert _numeric_peer_id("-102251057", "source channel") == -102251057
    assert _numeric_peer_id("@102251057", "source channel") == 102251057

    with pytest.raises(EitaaError) as excinfo:
        _numeric_peer_id("@testgroup1", "destination group")
    assert excinfo.value.permanent is True
    assert "numeric peer id" in excinfo.value.message


@pytest.mark.asyncio
async def test_repeat_deliveries_reach_the_daily_report():
    """Repeat/bundle sends must be counted, not silently omitted.

    With bundling on, nine of every ten forwards go out through the repeat
    path. It wrote no SendLog, and the daily report counts SendLog rows, so a
    night that delivered 421 messages was reported to the client as 0 sent.
    """
    from reporter import collect_stats

    await _prepare()
    fake = FakeTransport()
    worker = QueueWorker(fake)  # type: ignore[arg-type]
    async with session_scope() as session:
        group = Group(chat_id="-5001")
        session.add(group)
        await session.flush()
        post = Post(
            source_chat_id="-100111", message_id=900, kind="text", text="hi",
            ready_at=utcnow(), dispatched=True,
        )
        session.add(post)
        await session.flush()
        item = RepeatItem(post_id=post.id, group_id=group.id, round=0)
        session.add(item)
        await session.flush()
        ok_id, fail_id = item.id, None
        second = RepeatItem(post_id=post.id, group_id=group.id, round=1)
        session.add(second)
        await session.flush()
        fail_id = second.id

    async with session_scope() as session:
        ok_item = await session.get(RepeatItem, ok_id)
        bad_item = await session.get(RepeatItem, fail_id)
    await worker._finalize_repeat(ok_item, ok=True, error=None)
    await worker._finalize_repeat(bad_item, ok=False, error="CHAT_WRITE_FORBIDDEN", permanent=True)

    async with session_scope() as session:
        logs = (await session.execute(select(SendLog))).scalars().all()
    assert len(logs) == 2, "both outcomes must be logged"
    assert {log.ok for log in logs} == {True, False}
    assert all(log.queue_item_id is None for log in logs)

    stats = await collect_stats()
    assert stats["sent_ok"] == 1
    assert stats["sent_failed"] == 1
    assert stats["last_success_at"] is not None


@pytest.mark.asyncio
async def test_bundle_follows_the_channel_not_the_bots_own_gaps():
    """The bug the client caught: a week-old post arriving beside today's news.

    `posts` only holds what was ingested while the bot was up, and every
    restart skips whatever was published during the downtime. So "the 9 rows
    before this one" resolved to messages sixty and three hundred places back
    in the real channel. Predecessors must be the channel's, whether or not
    the bot ever saw them.
    """
    await _prepare()
    # The channel published 20..12 since the bot last ingested anything; its
    # own table knows only message 10, from days earlier.
    channel = [20, 19, 18, 17, 16, 15, 14, 13, 12, 10]
    fake = FakeTransport(channel_ids=channel)
    worker = QueueWorker(fake)  # type: ignore[arg-type]
    worker.settings.bundle_enabled = True
    worker.settings.bundle_previous_count = 9

    async with session_scope() as session:
        session.add(Group(chat_id="-5001"))
        session.add(
            Post(
                source_chat_id="-100111", message_id=10, kind="text", text="stale",
                ready_at=utcnow(), dispatched=True,
                created_at=utcnow() - dt.timedelta(days=7),
            )
        )
        session.add(
            Post(
                source_chat_id="-100111", message_id=21, kind="text", text="today",
                ready_at=utcnow(), dispatched=False,
            )
        )

    await worker.dispatch_due_posts()

    async with session_scope() as session:
        bundled = (
            await session.execute(
                select(Post.message_id)
                .join(RepeatItem, RepeatItem.post_id == Post.id)
                .where(RepeatItem.round >= BUNDLE_ROUND_OFFSET)
            )
        ).scalars().all()

    assert sorted(bundled) == [12, 13, 14, 15, 16, 17, 18, 19, 20, 21], (
        "the batch is the nine channel messages before this post, plus the post"
    )
    assert 10 not in bundled, "the week-old row the bot happens to hold is not a predecessor"
    assert fake.history_calls, "predecessors must be read from the channel"
    assert fake.history_calls[0]["before_id"] == 21


@pytest.mark.asyncio
async def test_idle_rounds_step_back_through_the_channel():
    """Round 0 is the channel's newest 10, round 1 the 10 before those."""
    from repeater import Repeater

    await _prepare()
    channel = list(range(100, 70, -1))  # 100..71, newest first
    fake = FakeTransport(channel_ids=channel)
    worker = QueueWorker(fake)  # type: ignore[arg-type]
    repeater = Repeater(transport=fake, worker=worker)
    repeater.settings.repeat_enabled = True
    repeater.settings.repeat_idle_hours = 1
    repeater.settings.repeat_last_n_posts = 10
    _open_active_window(repeater.settings)
    repeater._cycle = 1
    async with session_scope() as session:
        session.add(Group(chat_id="-5001"))
    await settings_store.set("repeat_rounds_done", 0)

    async def rounds_for(hours: int) -> list[int]:
        repeater._started_at = utcnow() - dt.timedelta(hours=hours, minutes=1)
        repeater._anchor_seen_at = utcnow() - dt.timedelta(hours=hours, minutes=1)
        await repeater.tick()
        async with session_scope() as session:
            return (
                await session.execute(
                    select(Post.message_id)
                    .join(RepeatItem, RepeatItem.post_id == Post.id)
                    .where(RepeatItem.round < BUNDLE_ROUND_OFFSET)
                )
            ).scalars().all()

    await repeater.tick()  # first tick records the anchor
    after_one_hour = sorted(await rounds_for(1), reverse=True)
    assert after_one_hour == channel[:10], "round 0 is the channel's newest ten"

    after_two_hours = sorted(await rounds_for(2), reverse=True)
    assert after_two_hours == channel[:20], "round 1 adds the ten before those"


@pytest.mark.asyncio
async def test_repeats_go_out_one_group_at_a_time_in_channel_order():
    """A group should get its whole batch together, oldest message first.

    Rows are created post by post, so ordering by id sent message 1 to all
    forty groups, then message 2 to all forty. A group saw one message every
    forty sends, and the batch took the whole queue to land.
    """
    await _prepare()
    fake = FakeTransport()
    worker = QueueWorker(fake)  # type: ignore[arg-type]

    async with session_scope() as session:
        groups = []
        for chat in ("-5001", "-5002", "-5003"):
            g = Group(chat_id=chat)
            session.add(g)
            await session.flush()
            groups.append(g.id)
        posts = []
        for message_id in (100, 101, 102):
            p = Post(
                source_chat_id="-100111", message_id=message_id, kind="text",
                text=f"m{message_id}", ready_at=utcnow(), dispatched=True,
            )
            session.add(p)
            await session.flush()
            posts.append(p.id)
        # Written post-major, exactly as the bundle writes them.
        for post_id in posts:
            for group_id in groups:
                session.add(RepeatItem(post_id=post_id, group_id=group_id, round=7))

    order: list[tuple[int, int]] = []
    while True:
        item = await worker._claim_next_repeat_item()
        if item is None:
            break
        order.append((item.group_id, item.post_id))

    assert len(order) == 9
    # Each group's three messages must be consecutive...
    for group_id in groups:
        positions = [i for i, (g, _) in enumerate(order) if g == group_id]
        assert positions == list(range(positions[0], positions[0] + 3)), (
            f"group {group_id} was interrupted by another group"
        )
    # ...and in ascending post order, which is channel order.
    for group_id in groups:
        seen = [p for g, p in order if g == group_id]
        assert seen == sorted(seen), "a group's copies must follow channel order"


@pytest.mark.asyncio
async def test_whole_batch_reaches_a_group_in_channel_order():
    """The client's requirement, end to end: group by group, oldest first.

    Two things broke this live. The triggering post was queued as a QueueItem
    and went out through the other sender loop, which runs concurrently, so it
    landed in the middle of its own batch; and repeats were ordered by row id,
    which for stub rows created newest-first is message id descending. A group
    received 17567, 17568, 17572, 17569, 17570, 17571, 17566, 17565, 17564,
    17563.
    """
    await _prepare()
    channel = [17572, 17571, 17570, 17569, 17568, 17567, 17566, 17565, 17564, 17563]
    fake = FakeTransport(channel_ids=channel)
    worker = QueueWorker(fake)  # type: ignore[arg-type]
    worker.settings.bundle_enabled = True
    worker.settings.bundle_previous_count = 9

    async with session_scope() as session:
        for chat in ("-5001", "-5002", "-5003"):
            session.add(Group(chat_id=chat))
        session.add(
            Post(
                source_chat_id="-100111", message_id=17572, kind="text",
                text="newest", ready_at=utcnow(), dispatched=False,
            )
        )

    await worker.dispatch_due_posts()

    async with session_scope() as session:
        queue_rows = (await session.execute(select(QueueItem))).scalars().all()
    assert queue_rows == [], "a bundled post must not also go through the queue loop"

    order: list[tuple[int, int]] = []
    while True:
        item = await worker._claim_next_repeat_item()
        if item is None:
            break
        async with session_scope() as session:
            message_id = (await session.get(Post, item.post_id)).message_id
        order.append((item.group_id, message_id))

    assert len(order) == 30, "ten messages for each of three groups"
    groups_in_order = []
    for group_id, _ in order:
        if not groups_in_order or groups_in_order[-1] != group_id:
            groups_in_order.append(group_id)
    assert len(groups_in_order) == 3, "a group's batch must not be interrupted"

    for group_id in groups_in_order:
        messages = [m for g, m in order if g == group_id]
        assert messages == sorted(channel), (
            f"group {group_id} got {messages}, not the channel's order oldest first"
        )


@pytest.mark.asyncio
async def test_only_one_loop_rebuilds_a_lost_browser():
    """Concurrent ensure_session calls must not all restart the browser.

    Ingest, the sender and the repeat sender each call ensure_session. With no
    lock they all tore down and rebuilt the same browser at once, and the live
    log showed "web session restored on attempt 1" followed in the same second
    by "web session lost; restart attempt 1/3".
    """
    from web_transport import WebTransport

    transport = WebTransport()
    healthy = {"value": False}
    restarts = {"count": 0}

    async def fake_healthy() -> bool:
        return healthy["value"]

    async def fake_restart() -> None:
        restarts["count"] += 1
        await asyncio.sleep(0.05)   # a real restart is not instant
        healthy["value"] = True

    transport._healthy = fake_healthy          # type: ignore[assignment]
    transport._restart_session = fake_restart  # type: ignore[assignment]

    await asyncio.gather(*(transport.ensure_session() for _ in range(3)))

    assert restarts["count"] == 1, (
        f"three loops triggered {restarts['count']} browser restarts"
    )


@pytest.mark.asyncio
async def test_new_post_clears_an_earlier_posts_bundle_too():
    """Client rule 3: a new post wipes the whole outstanding queue.

    Cancelling only idle repeats was not enough. On a channel that publishes
    twice in two minutes, the previous post's bundle was still draining and
    went out beside the new one, which is what made groups look spammed.
    """
    await _prepare()
    fake = FakeTransport(channel_ids=[50, 49, 48, 47, 46, 45])
    worker = QueueWorker(fake)  # type: ignore[arg-type]
    worker.settings.bundle_enabled = True
    worker.settings.bundle_previous_count = 5

    async with session_scope() as session:
        group = Group(chat_id="-5001")
        session.add(group)
        await session.flush()
        older = Post(
            source_chat_id="-100111", message_id=40, kind="text", text="older",
            ready_at=utcnow(), dispatched=True,
        )
        session.add(older)
        await session.flush()
        # Left over from the previous post: one idle repeat and one bundle row.
        session.add(RepeatItem(post_id=older.id, group_id=group.id, round=3, status="pending"))
        session.add(
            RepeatItem(post_id=older.id, group_id=group.id,
                       round=BUNDLE_ROUND_OFFSET + older.id, status="pending")
        )
        session.add(QueueItem(post_id=older.id, group_id=group.id, status="pending"))
        session.add(
            Post(
                source_chat_id="-100111", message_id=50, kind="text", text="new",
                ready_at=utcnow(), dispatched=False,
            )
        )
        older_id = older.id

    await worker.dispatch_due_posts()

    async with session_scope() as session:
        stale_repeats = (
            await session.execute(select(RepeatItem).where(RepeatItem.post_id == older_id))
        ).scalars().all()
        stale_queue = (
            await session.execute(select(QueueItem).where(QueueItem.post_id == older_id))
        ).scalars().all()
        fresh = (
            await session.execute(
                select(RepeatItem).where(RepeatItem.status == "pending")
            )
        ).scalars().all()

    assert all(r.status == "skipped" for r in stale_repeats), (
        "the previous post's idle repeat AND its bundle must both be dropped"
    )
    assert all(q.status == "skipped" for q in stale_queue)
    assert fresh, "the new post's own batch must survive the clear-out"
    assert len(fresh) == 6, "new post plus five predecessors, for one group"
