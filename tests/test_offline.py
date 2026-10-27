from __future__ import annotations

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

    def __init__(self) -> None:
        self.sent: list[tuple[str, Any]] = []
        self.groups: list[tuple[str, Sequence[dict[str, Any]]]] = []

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
    worker = QueueWorker(FakeTransport())  # type: ignore[arg-type]
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
