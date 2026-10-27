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
            # is_logged_in() alone is not enough: a profile whose session the
            # server already dropped still renders its cached chat list, so it
            # would report success the moment the window opened and nobody
            # would actually be signed in. Wait for the server to accept it.
            if await transport.is_logged_in():
                session_ok, _ = await transport.verify_api_session()
                if session_ok:
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
        # "logged in" above is only the cached DOM; ask the server too.
        session_ok, session_error = await transport.verify_api_session()
        print("server session:", "valid" if session_ok else f"REJECTED ({session_error})")
        if not session_ok:
            print(
                "\nthe UI renders from cache but the server dropped this session: "
                "run `python manage.py login` and sign in again"
            )
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


AUDIT_SCRIPT = """
const cb = arguments[arguments.length - 1];
const ids = arguments[0];
(async () => {
  // Load every dialog the account has before judging anything: an unloaded
  // peer answers PEER_ID_INVALID exactly like one the account was never in,
  // and a single 100-item page does not cover an account in many groups.
  try {
    for (const folder of [0, 1]) {
      let offsetDate = 0, offsetId = 0, offsetPeer = {_: 'inputPeerEmpty'}, page = 0;
      while (page++ < 20) {
        const r = await apiManager.invokeApi('messages.getDialogs', {
          offset_date: offsetDate, offset_id: offsetId, offset_peer: offsetPeer,
          limit: 100, hash: 0, folder_id: folder});
        const dialogs = r.dialogs || [];
        if (dialogs.length < 100) break;
        const last = dialogs[dialogs.length - 1];
        offsetId = last.top_message || 0;
        offsetDate = last.date || 0;
        try { offsetPeer = appPeersManager.getInputPeerById(
          appPeersManager.getPeerId ? appPeersManager.getPeerId(last.peer) : 0); } catch (e) { break; }
      }
    }
  } catch (e) { /* fall through: per-peer probe below is the real test */ }

  const out = {};
  for (const id of ids) {
    let peer = null;
    try { peer = appPeersManager.getPeer(id); } catch (e) {}
    const known = peer && peer._ && peer._ !== 'chatEmpty';
    if (!known) {
      // Confirm with the server rather than trusting local state.
      // Probe twice: a peer the client has not settled on yet answers
      // PEER_ID_INVALID once and resolves fine a moment later, and this
      // verdict can disable a working group.
      let verdict = null, why = '';
      for (let attempt = 0; attempt < 2; attempt++) {
        try {
          await apiManager.invokeApi('messages.getHistory', {
            peer: appPeersManager.getInputPeerById(id),
            offset_id: 0, offset_date: 0, add_offset: 0,
            limit: 1, max_id: 0, min_id: 0, hash: 0});
          verdict = 'ok'; why = 'reachable';
          break;
        } catch (e) {
          why = (e && (e.type || e.error_message)) || String(e);
          verdict = 'unreachable';
          if (attempt === 0) await new Promise(r => setTimeout(r, 400));
        }
      }
      out[id] = {verdict: verdict, why: why};
      continue;
    }
    const f = peer.pFlags || {};
    const banned = (peer.banned_rights && peer.banned_rights.pFlags) || {};
    const defaultBanned = (peer.default_banned_rights && peer.default_banned_rights.pFlags) || {};
    const privileged = !!peer.admin_rights || !!f.creator;
    let verdict = 'ok', why = '';
    if (f.left || f.kicked || f.deactivated) { verdict = 'unreachable'; why = 'left/kicked/deactivated'; }
    else if (banned.send_messages) { verdict = 'cannot_post'; why = 'this account is banned from posting'; }
    else if (defaultBanned.send_messages && !privileged) { verdict = 'cannot_post'; why = 'members cannot post'; }
    else if (f.broadcast && !privileged) { verdict = 'cannot_post'; why = 'broadcast channel, not an admin'; }
    out[id] = {verdict: verdict, why: why, title: peer.title || ''};
  }
  cb(out);
})();
"""


async def cmd_audit(args: argparse.Namespace) -> int:
    """Advisory check of group deliverability that sends nothing.

    A forward only lands where the account is still a member and allowed to
    post, and neither fact surfaces in the queue until a send has already
    failed. Reading it up front is useful, but only as a hint: the verdict
    comes from the web client's own peer state, and repeated runs disagree
    with each other (54/48/9 reachable across three runs on the same data)
    because peers resolve or not depending on what the client happens to have
    loaded. Ground truth is still an actual delivery, so this never edits the
    database -- it only reports.
    """
    from web_transport import WebTransport

    async with session_scope() as session:
        groups = (await session.execute(select(Group).order_by(Group.id))).scalars().all()
        rows = [(g.id, g.chat_id, g.title) for g in groups]
    if not rows:
        print("no groups")
        return 0

    numeric: dict[int, int] = {}
    bad_id: list[tuple[int, str]] = []
    for gid, chat_id, _title in rows:
        try:
            numeric[gid] = int(str(chat_id).lstrip("@"))
        except ValueError:
            bad_id.append((gid, chat_id))

    transport = WebTransport(get_settings())
    await transport.start(headless=args.headless)
    try:
        session_ok, session_error = await transport.verify_api_session()
        if not session_ok:
            print(f"session rejected by the server ({session_error}); run `manage.py login`")
            return 1
        driver = transport._require_driver()  # noqa: SLF001

        def _audit():
            driver.set_script_timeout(180)
            return driver.execute_async_script(AUDIT_SCRIPT, list(numeric.values()))

        report = await transport._run(_audit, op="audit")  # noqa: SLF001
    finally:
        await transport.aclose()

    buckets: dict[str, list[str]] = {"ok": [], "cannot_post": [], "unreachable": [], "bad_id": []}
    verdicts: dict[int, str] = {}
    for gid, chat_id, title in rows:
        if gid not in numeric:
            buckets["bad_id"].append(f"{chat_id} | {title or '-'}")
            verdicts[gid] = "bad_id"
            continue
        info = report.get(str(numeric[gid])) or report.get(numeric[gid]) or {}
        verdict = info.get("verdict", "unreachable")
        why = info.get("why", "no answer")
        verdicts[gid] = verdict
        line = f"{chat_id} | {(title or '-')[:34]}"
        buckets[verdict].append(line if verdict == "ok" else f"{line} [{why}]")

    total = len(rows)
    print(f"{total} groups: {len(buckets['ok'])} deliverable, "
          f"{len(buckets['cannot_post'])} cannot post, "
          f"{len(buckets['unreachable'])} unreachable, {len(buckets['bad_id'])} bad id")
    for name in ("cannot_post", "unreachable", "bad_id"):
        if not buckets[name]:
            continue
        print(f"\n{name} ({len(buckets[name])}):")
        for line in buckets[name]:
            print(f"  {line}")

    print(
        "\nNOTE: this is advisory only. The client-side peer state it reads is "
        "not stable between runs, so treat a single verdict as a hint, not proof. "
        "The reliable signal is an actual send: check `manage.py list` after a run."
    )
    return 0


async def cmd_sync_groups(args: argparse.Namespace) -> int:
    """Make the group list exactly the chats in a file of invite links.

    The client maintains the destination list as eitaa.com/joinchat links, but
    everything downstream forwards by numeric peer id, so each link has to be
    resolved against the account's own membership first. Chats already in the
    database keep their history; ones missing from the file are disabled rather
    than deleted, so a mistake in the file is recoverable.
    """
    import re as _re

    from web_transport import WebTransport

    raw_lines = [
        line.strip()
        for line in Path(args.file).read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]
    hashes: list[str] = []
    plain: list[str] = []
    for line in raw_lines:
        match = _re.search(r"joinchat/([A-Za-z0-9_-]+)", line)
        if match:
            if match.group(1) not in hashes:
                hashes.append(match.group(1))
        else:
            normalized = normalize_chat_id(line)
            if normalized and normalized not in plain:
                plain.append(normalized)
    print(f"{len(raw_lines)} lines -> {len(hashes)} unique invite links, {len(plain)} direct ids")
    if len(raw_lines) != len(hashes) + len(plain):
        print(f"  ({len(raw_lines) - len(hashes) - len(plain)} duplicate line(s) collapsed)")

    resolved: dict[str, dict] = {}
    if hashes:
        transport = WebTransport(get_settings())
        await transport.start(headless=args.headless)
        try:
            session_ok, session_error = await transport.verify_api_session()
            if not session_ok:
                print(f"session rejected by the server ({session_error}); run `manage.py login`")
                return 1
            print("resolving invite links (about a second each)...")
            resolved = await transport.resolve_invite_hashes(hashes)
        finally:
            await transport.aclose()

    wanted: dict[str, str] = {}      # chat_id -> title
    failures: list[tuple[str, str]] = []
    for h in hashes:
        info = resolved.get(h) or {}
        if info.get("id"):
            wanted[str(-abs(int(info["id"])))] = info.get("title") or ""
        else:
            failures.append((h, info.get("error", "no answer")))
    for chat_id in plain:
        wanted.setdefault(chat_id, "")

    print(f"\nresolved {len(wanted)} chats, {len(failures)} failed")
    for h, why in failures:
        print(f"  FAILED {h}: {why}")

    if not wanted:
        print("nothing resolved; leaving the database alone")
        return 1

    added = reactivated = disabled = 0
    async with session_scope() as session:
        existing = {
            g.chat_id: g
            for g in (await session.execute(select(Group))).scalars().all()
        }
        for chat_id, title in wanted.items():
            group = existing.get(chat_id)
            if group is None:
                session.add(Group(chat_id=chat_id, title=title or None))
                added += 1
                continue
            if not group.enabled or group.status != "active":
                group.enabled = True
                group.status = "active"
                group.last_error = None
                group.fail_count = 0
                reactivated += 1
            if title and not group.title:
                group.title = title
        for chat_id, group in existing.items():
            if chat_id not in wanted and group.enabled:
                group.enabled = False
                group.status = "removed"
                group.last_error = "not in the current group list"
                disabled += 1

    print(f"\nadded {added}, re-enabled {reactivated}, disabled {disabled} no longer listed")
    async with session_scope() as session:
        total = (await session.execute(select(func.count(Group.id)))).scalar_one()
        active = (
            await session.execute(select(func.count(Group.id)).where(Group.enabled.is_(True)))
        ).scalar_one()
    print(f"group list now: {active} enabled of {total}")
    return 0


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

    sync = sub.add_parser("sync-groups", help="replace the group list from a file of invite links")
    sync.add_argument("file")
    sync.add_argument("--headless", action="store_true")
    sync.set_defaults(func=cmd_sync_groups)

    audit = sub.add_parser("audit", help="check every group for deliverability (sends nothing)")
    audit.add_argument("--headless", action="store_true")
    audit.set_defaults(func=cmd_audit)
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
