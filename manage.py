"""Command-line control panel.

The eitaayar gateway has no getUpdates/webhook, so a bot running in that mode
can never receive an admin command inside Eitaa. This CLI exposes the same
operations over the same database, and `publish` feeds a post into the very
same queue the channel ingestor would use.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
from pathlib import Path

from sqlalchemy import func, select

from config import get_settings, setup_logging, ensure_utf8_console
from db import (
    DEFAULTS,
    Group,
    Post,
    PostMedia,
    QueueItem,
    RepeatItem,
    init_db,
    session_scope,
    settings_store,
    utcnow,
)
from commands import (
    add_managed_group,
    normalize_chat_id,
    remove_managed_group,
    set_managed_group_enabled,
)
from eitaa_client import EitaaClient, EitaaError
from reporter import collect_stats, format_report


async def _prepare() -> None:
    await init_db()
    await settings_store.load()
    for key, default in DEFAULTS.items():
        if not await settings_store.has(key):
            await settings_store.set(key, default)


async def cmd_check(args: argparse.Namespace) -> int:
    client = EitaaClient(get_settings())
    try:
        me = await client.get_me()
        print("token OK ->", me)
        return 0
    except EitaaError as exc:
        print("token check failed:", exc.message)
        return 1
    finally:
        await client.aclose()


async def cmd_login(args: argparse.Namespace) -> int:
    """Open a visible browser so the account owner can sign in themselves.

    Deliberately interactive: nobody has to hand over a phone number or a
    verification code. The session then lives in the Chrome profile directory.
    """
    from web_transport import WebTransport

    transport = WebTransport(get_settings())
    await transport.start(headless=False)
    print("A Chrome window is open on web.eitaa.com.")
    print("Sign in there yourself (QR or phone + code). Waiting...")
    try:
        deadline = asyncio.get_running_loop().time() + args.timeout
        while asyncio.get_running_loop().time() < deadline:
            if await transport.is_logged_in():
                print("signed in; session saved to", get_settings().chrome_profile_dir)
                print("strategy:", await transport.detect_strategy())
                return 0
            await asyncio.sleep(3)
        print("timed out waiting for sign-in")
        return 1
    finally:
        await transport.aclose()


async def cmd_probe(args: argparse.Namespace) -> int:
    """Report what the live web client actually exposes."""
    from web_transport import WebTransport

    transport = WebTransport(get_settings())
    await transport.start(headless=args.headless)
    try:
        result = await transport.probe()
        print("url:        ", result["url"])
        print("logged in:  ", result["logged_in"])
        print("strategy:   ", result["strategy"])
        print("selectors:")
        for name, count in result["selectors"].items():
            mark = "ok " if isinstance(count, int) and count > 0 else "MISS"
            print(f"  [{mark}] {name:<16} {transport.selectors[name]!r} -> {count}")
        if not result["logged_in"]:
            print("\nnot signed in yet: run `python manage.py login` first")
            return 1
        return 0
    finally:
        await transport.aclose()


async def cmd_add(args: argparse.Namespace) -> int:
    code, message = await add_managed_group(args.chat_id, args.title)
    print(message)
    return code


async def cmd_import(args: argparse.Namespace) -> int:
    """Bulk-add groups from a text file, one id/link per line."""
    added = 0
    for line in Path(args.file).read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        chat_id = normalize_chat_id(line) or line
        async with session_scope() as session:
            exists = (
                await session.execute(select(Group).where(Group.chat_id == chat_id))
            ).scalar_one_or_none()
            if exists is None:
                session.add(Group(chat_id=chat_id))
                added += 1
    print(f"imported {added} new groups")
    return 0


async def cmd_remove(args: argparse.Namespace) -> int:
    code, message = await remove_managed_group(args.chat_id)
    print(message)
    return code


async def cmd_enable(args: argparse.Namespace) -> int:
    code, message = await set_managed_group_enabled(args.chat_id, True)
    print(message)
    return code


async def cmd_disable(args: argparse.Namespace) -> int:
    code, message = await set_managed_group_enabled(args.chat_id, False)
    print(message)
    return code


async def cmd_list(args: argparse.Namespace) -> int:
    async with session_scope() as session:
        groups = (await session.execute(select(Group).order_by(Group.id))).scalars().all()
        rows = [
            (g.chat_id, g.title, g.enabled, g.status, (g.last_error or "")[:70]) for g in groups
        ]
    if not rows:
        print("no groups")
        return 0
    print(f"{len(rows)} groups")
    for chat_id, title, enabled, status, error in rows:
        flag = "on " if enabled else "off"
        print(f"[{flag}] {chat_id:<24} {status:<8} {title or '-':<24} {error}")
    return 0


async def cmd_status(args: argparse.Namespace) -> int:
    async with session_scope() as session:
        counts = {}
        for state in ("pending", "sending", "done", "failed"):
            counts[state] = (
                await session.execute(
                    select(func.count(QueueItem.id)).where(QueueItem.status == state)
                )
            ).scalar_one()
        waiting = (
            await session.execute(select(func.count(Post.id)).where(Post.dispatched.is_(False)))
        ).scalar_one()
        repeat_counts = {}
        for state in ("pending", "sending", "done", "failed"):
            repeat_counts[state] = (
                await session.execute(
                    select(func.count(RepeatItem.id)).where(RepeatItem.status == state)
                )
            ).scalar_one()
    print("paused:", await settings_store.get_bool("paused"))
    print("delay:", await settings_store.get_int("delay_seconds"), "s")
    print("interval:", await settings_store.get_float("send_interval"), "s")
    print("max/minute:", await settings_store.get_int("max_per_minute"))
    print("posts waiting:", waiting)
    print("queue:", counts)
    settings = get_settings()
    if settings.repeat_enabled:
        anchor = await settings_store.get_int("repeat_anchor_post_id")
        rounds = await settings_store.get_int("repeat_rounds_done")
        print(
            f"repeat: enabled (idle={settings.repeat_idle_hours}h, n={settings.repeat_last_n_posts}) "
            f"| anchor_post={anchor} rounds_done={rounds}"
        )
        print("repeat queue:", repeat_counts)
    else:
        print("repeat: disabled")
    return 0


async def cmd_report(args: argparse.Namespace) -> int:
    print(format_report(await collect_stats()))
    return 0


async def cmd_set(args: argparse.Namespace) -> int:
    allowed = {"delay_seconds", "send_interval", "max_per_minute", "paused"}
    if args.key not in allowed:
        print("key must be one of:", ", ".join(sorted(allowed)))
        return 1
    await settings_store.set(args.key, args.value)
    print(f"{args.key} = {args.value}")
    return 0


async def cmd_repeat_test(args: argparse.Namespace) -> int:
    """Force one repeater tick as if the idle threshold had just elapsed.

    Does this by temporarily backdating the latest real post's created_at,
    running the normal tick logic unchanged, then restoring it — so this
    exercises the exact same code path production will use, not a copy.
    """
    from repeater import Repeater

    async with session_scope() as session:
        latest = (
            await session.execute(
                select(Post).where(Post.message_id > 0).order_by(Post.id.desc()).limit(1)
            )
        ).scalar_one_or_none()
        if latest is None:
            print("no real channel posts captured yet")
            return 1
        original_created_at = latest.created_at
        settings = get_settings()
        latest.created_at = utcnow() - dt.timedelta(
            hours=settings.repeat_idle_hours * (args.rounds), seconds=5
        )

    try:
        await Repeater(get_settings()).tick()
    finally:
        async with session_scope() as session:
            row = await session.get(Post, latest.id)
            if row is not None:
                row.created_at = original_created_at
    print(f"forced tick as if idle for {args.rounds} round(s); check `status` for repeat queue counts")
    return 0


async def cmd_retry(args: argparse.Namespace) -> int:
    async with session_scope() as session:
        items = (
            await session.execute(select(QueueItem).where(QueueItem.status == "failed"))
        ).scalars().all()
        for item in items:
            item.status = "pending"
            item.attempts = 0
            item.next_attempt_at = utcnow()
            item.last_error = None
    print(f"requeued {len(items)} failed items")
    return 0


async def cmd_publish(args: argparse.Namespace) -> int:
    """Push a post into the queue without needing getUpdates."""
    settings = get_settings()
    if not args.text and not args.file:
        print("give --text and/or --file")
        return 1

    delay = args.delay if args.delay is not None else await settings_store.get_int("delay_seconds")
    async with session_scope() as session:
        seq = (await session.execute(select(func.max(Post.id)))).scalar_one() or 0
        post = Post(
            source_chat_id=str(settings.source_channel_id),
            message_id=-(seq + 1),  # negative ids never collide with channel ids
            kind="media" if args.file else "text",
            text=args.text,
            ready_at=utcnow() + dt.timedelta(seconds=max(delay, 0)),
        )
        session.add(post)
        await session.flush()
        for position, path in enumerate(args.file or []):
            file_path = Path(path)
            if not file_path.exists():
                print("missing file:", file_path)
                return 1
            session.add(
                PostMedia(
                    post_id=post.id,
                    position=position,
                    media_type=args.type,
                    local_path=str(file_path.resolve()),
                    file_name=file_path.name,
                    caption=args.text if position == 0 else None,
                )
            )
        if args.file:
            post.kind = "album" if len(args.file) > 1 else "media"
        post_id = post.id
    print(f"queued post {post_id}; it will be sent in {delay}s")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="manage.py", description="Eitaa forwarder control panel")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("check", help="verify the API token with getMe").set_defaults(func=cmd_check)

    login = sub.add_parser("login", help="open a browser for the account owner to sign in")
    login.add_argument("--timeout", type=int, default=600)
    login.set_defaults(func=cmd_login)

    probe = sub.add_parser("probe", help="report live selectors and send strategy")
    probe.add_argument("--headless", action="store_true")
    probe.set_defaults(func=cmd_probe)

    add = sub.add_parser("add", help="add a destination group")
    add.add_argument("chat_id")
    add.add_argument("--title", default=None)
    add.set_defaults(func=cmd_add)

    imp = sub.add_parser("import", help="add many groups from a text file")
    imp.add_argument("file")
    imp.set_defaults(func=cmd_import)

    rem = sub.add_parser("remove", help="remove a group")
    rem.add_argument("chat_id")
    rem.set_defaults(func=cmd_remove)

    for name, func in (("enable", cmd_enable), ("disable", cmd_disable)):
        item = sub.add_parser(name, help=f"{name} a group")
        item.add_argument("chat_id")
        item.set_defaults(func=func)

    sub.add_parser("list", help="list groups").set_defaults(func=cmd_list)
    sub.add_parser("status", help="queue and settings snapshot").set_defaults(func=cmd_status)
    sub.add_parser("report", help="today's report").set_defaults(func=cmd_report)
    sub.add_parser("retry", help="requeue failed items").set_defaults(func=cmd_retry)

    repeat_test = sub.add_parser(
        "repeat-test", help="force a repeater tick as if N idle hours had elapsed"
    )
    repeat_test.add_argument("--rounds", type=int, default=1)
    repeat_test.set_defaults(func=cmd_repeat_test)

    setter = sub.add_parser("set", help="change a runtime setting")
    setter.add_argument("key")
    setter.add_argument("value")
    setter.set_defaults(func=cmd_set)

    publish = sub.add_parser("publish", help="queue a post for all enabled groups")
    publish.add_argument("--text", default=None)
    publish.add_argument("--file", action="append", default=None)
    publish.add_argument("--type", default="document", help="photo/video/audio/voice/document")
    publish.add_argument("--delay", type=int, default=None)
    publish.set_defaults(func=cmd_publish)

    return parser


async def main_async(args: argparse.Namespace) -> int:
    setup_logging()
    await _prepare()
    return await args.func(args)


def main() -> int:
    ensure_utf8_console()
    args = build_parser().parse_args()
    return asyncio.run(main_async(args))


if __name__ == "__main__":
    raise SystemExit(main())
