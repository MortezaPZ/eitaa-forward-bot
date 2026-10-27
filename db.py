from __future__ import annotations

import datetime as dt
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Optional

from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    select,
)
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from config import get_settings


def utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)


class Base(DeclarativeBase):
    pass


class Group(Base):
    """A destination group the bot mirrors channel posts into."""

    __tablename__ = "groups"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    chat_id: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    title: Mapped[Optional[str]] = mapped_column(String(256), default=None)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    status: Mapped[str] = mapped_column(String(32), default="active")
    last_error: Mapped[Optional[str]] = mapped_column(Text, default=None)
    last_error_at: Mapped[Optional[dt.datetime]] = mapped_column(DateTime, default=None)
    fail_count: Mapped[int] = mapped_column(Integer, default=0)
    last_sent_at: Mapped[Optional[dt.datetime]] = mapped_column(DateTime, default=None)
    created_at: Mapped[dt.datetime] = mapped_column(DateTime, default=utcnow)


class Post(Base):
    """A captured channel post (single message or a whole media album)."""

    __tablename__ = "posts"
    __table_args__ = (UniqueConstraint("source_chat_id", "message_id", name="uq_post_source_msg"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    source_chat_id: Mapped[str] = mapped_column(String(128), index=True)
    message_id: Mapped[int] = mapped_column(Integer, index=True)
    media_group_id: Mapped[Optional[str]] = mapped_column(String(128), index=True, default=None)
    kind: Mapped[str] = mapped_column(String(16), default="text")
    text: Mapped[Optional[str]] = mapped_column(Text, default=None)
    raw: Mapped[Optional[str]] = mapped_column(Text, default=None)
    created_at: Mapped[dt.datetime] = mapped_column(DateTime, default=utcnow)
    edited_at: Mapped[Optional[dt.datetime]] = mapped_column(DateTime, default=None)
    ready_at: Mapped[dt.datetime] = mapped_column(DateTime, default=utcnow, index=True)
    dispatched: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    dispatched_at: Mapped[Optional[dt.datetime]] = mapped_column(DateTime, default=None)
    # Set when this post is a scheduled re-run of an earlier one (repeat feature).
    repeat_of_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("posts.id", ondelete="SET NULL"), default=None
    )

    media: Mapped[list["PostMedia"]] = relationship(
        back_populates="post",
        cascade="all, delete-orphan",
        order_by="PostMedia.position",
        lazy="selectin",
    )


class PostMedia(Base):
    """One attachment belonging to a post; albums hold several ordered rows."""

    __tablename__ = "post_media"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    post_id: Mapped[int] = mapped_column(ForeignKey("posts.id", ondelete="CASCADE"), index=True)
    position: Mapped[int] = mapped_column(Integer, default=0)
    message_id: Mapped[Optional[int]] = mapped_column(Integer, default=None)
    media_type: Mapped[str] = mapped_column(String(24))
    file_id: Mapped[Optional[str]] = mapped_column(String(512), default=None)
    file_name: Mapped[Optional[str]] = mapped_column(String(256), default=None)
    mime_type: Mapped[Optional[str]] = mapped_column(String(128), default=None)
    caption: Mapped[Optional[str]] = mapped_column(Text, default=None)
    local_path: Mapped[Optional[str]] = mapped_column(String(512), default=None)

    post: Mapped[Post] = relationship(back_populates="media")


class QueueItem(Base):
    """Persistent unit of work: deliver one post to one group."""

    __tablename__ = "queue_items"
    __table_args__ = (UniqueConstraint("post_id", "group_id", name="uq_queue_post_group"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    post_id: Mapped[int] = mapped_column(ForeignKey("posts.id", ondelete="CASCADE"), index=True)
    group_id: Mapped[int] = mapped_column(ForeignKey("groups.id", ondelete="CASCADE"), index=True)
    status: Mapped[str] = mapped_column(String(16), default="pending", index=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    next_attempt_at: Mapped[dt.datetime] = mapped_column(DateTime, default=utcnow, index=True)
    last_error: Mapped[Optional[str]] = mapped_column(Text, default=None)
    sent_at: Mapped[Optional[dt.datetime]] = mapped_column(DateTime, default=None)
    created_at: Mapped[dt.datetime] = mapped_column(DateTime, default=utcnow, index=True)


class RepeatItem(Base):
    """One re-forward of an already-delivered post during a quiet channel.

    Deliberately a separate table from QueueItem: QueueItem's uniqueness on
    (post_id, group_id) is what makes normal delivery idempotent, and a
    repeat is the opposite intent — the same post going out again. `round`
    identifies which hourly batch this belongs to (0 = the most recent 10
    real posts, 1 = the 10 before those, ...), so re-running the scheduler
    never creates duplicates for a round already dispatched.
    """

    __tablename__ = "repeat_items"
    __table_args__ = (
        UniqueConstraint("post_id", "group_id", "round", name="uq_repeat_post_group_round"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    post_id: Mapped[int] = mapped_column(ForeignKey("posts.id", ondelete="CASCADE"), index=True)
    group_id: Mapped[int] = mapped_column(ForeignKey("groups.id", ondelete="CASCADE"), index=True)
    round: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[str] = mapped_column(String(16), default="pending", index=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    next_attempt_at: Mapped[dt.datetime] = mapped_column(DateTime, default=utcnow, index=True)
    last_error: Mapped[Optional[str]] = mapped_column(Text, default=None)
    sent_at: Mapped[Optional[dt.datetime]] = mapped_column(DateTime, default=None)
    created_at: Mapped[dt.datetime] = mapped_column(DateTime, default=utcnow)


class SendLog(Base):
    """Append-only record of every delivery attempt, used by /report."""

    __tablename__ = "send_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    queue_item_id: Mapped[Optional[int]] = mapped_column(Integer, default=None)
    post_id: Mapped[Optional[int]] = mapped_column(Integer, index=True, default=None)
    group_id: Mapped[Optional[int]] = mapped_column(Integer, index=True, default=None)
    ok: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    error: Mapped[Optional[str]] = mapped_column(Text, default=None)
    latency_ms: Mapped[Optional[float]] = mapped_column(Float, default=None)
    created_at: Mapped[dt.datetime] = mapped_column(DateTime, default=utcnow, index=True)


class Setting(Base):
    """Mutable runtime settings changed through admin commands."""

    __tablename__ = "settings"

    key: Mapped[str] = mapped_column(String(64), primary_key=True)
    value: Mapped[str] = mapped_column(Text)
    updated_at: Mapped[dt.datetime] = mapped_column(DateTime, default=utcnow, onupdate=utcnow)


_settings = get_settings()
_engine = create_async_engine(_settings.database_url, echo=False, future=True)
SessionLocal = async_sessionmaker(_engine, expire_on_commit=False, class_=AsyncSession)


@asynccontextmanager
async def session_scope() -> AsyncIterator[AsyncSession]:
    session = SessionLocal()
    try:
        yield session
        await session.commit()
    except Exception:
        await session.rollback()
        raise
    finally:
        await session.close()


async def init_db() -> None:
    from pathlib import Path

    if _settings.database_url.startswith("sqlite"):
        db_path = _settings.database_url.split("///", 1)[-1]
        Path(db_path).resolve().parent.mkdir(parents=True, exist_ok=True)
    async with _engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def dispose_db() -> None:
    await _engine.dispose()


DEFAULTS: dict[str, Any] = {
    "delay_seconds": _settings.send_delay_seconds,
    "send_interval": _settings.send_interval_seconds,
    "max_per_minute": _settings.max_sends_per_minute,
    "paused": False,
    "update_offset": 0,
    "last_report_date": "",
}


class SettingsStore:
    """Small typed key/value store on top of the settings table."""

    def __init__(self) -> None:
        self._cache: dict[str, str] = {}
        self._loaded = False

    async def load(self) -> None:
        async with session_scope() as session:
            rows = (await session.execute(select(Setting))).scalars().all()
        self._cache = {row.key: row.value for row in rows}
        self._loaded = True

    async def _ensure(self) -> None:
        if not self._loaded:
            await self.load()

    async def has(self, key: str) -> bool:
        await self._ensure()
        return key in self._cache

    async def get_raw(self, key: str) -> str | None:
        await self._ensure()
        if key in self._cache:
            return self._cache[key]
        default = DEFAULTS.get(key)
        return None if default is None else str(default)

    async def set(self, key: str, value: Any) -> None:
        text = str(value)
        async with session_scope() as session:
            row = await session.get(Setting, key)
            if row is None:
                session.add(Setting(key=key, value=text))
            else:
                row.value = text
                row.updated_at = utcnow()
        self._cache[key] = text

    async def get_int(self, key: str) -> int:
        raw = await self.get_raw(key)
        try:
            return int(float(raw)) if raw is not None else 0
        except ValueError:
            return 0

    async def get_float(self, key: str) -> float:
        raw = await self.get_raw(key)
        try:
            return float(raw) if raw is not None else 0.0
        except ValueError:
            return 0.0

    async def get_bool(self, key: str) -> bool:
        raw = await self.get_raw(key)
        return str(raw).strip().lower() in {"1", "true", "yes", "on"}

    async def get_str(self, key: str) -> str:
        return await self.get_raw(key) or ""


settings_store = SettingsStore()
