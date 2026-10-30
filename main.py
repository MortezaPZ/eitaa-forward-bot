from __future__ import annotations

import asyncio
import logging
import signal
import sys

from commands import CommandRouter
from config import (
    ensure_utf8_console,
    get_settings,
    is_within_active_window,
    seconds_until_active_window,
    seconds_until_active_window_end,
    setup_logging,
)
from db import DEFAULTS, dispose_db, init_db, settings_store
from eitaa_client import EitaaError
from web_transport import SessionUnrecoverable, build_transport
from ingest import Ingestor
from web_ingest import WebIngestor
from queue_worker import QueueWorker
from reporter import Reporter, notify_admins
from repeater import Repeater

logger = logging.getLogger("eitaa.main")


async def seed_settings() -> None:
    await settings_store.load()
    for key, default in DEFAULTS.items():
        if not await settings_store.has(key):
            await settings_store.set(key, default)


async def run(stop_event: asyncio.Event | None = None) -> int:
    settings = get_settings()
    setup_logging(settings)

    if not settings.admin_ids:
        logger.warning("ADMIN_IDS is empty; no one will be able to control the bot")

    await init_db()
    await seed_settings()

    wait = seconds_until_active_window(settings)
    if wait > 0:
        logger.info("outside active window; waiting %.0fs until %02d:00", wait, settings.active_start_hour)
        await asyncio.sleep(wait)

    client = await build_transport(settings)
    worker = QueueWorker(client, settings)
    is_web = settings.transport == "web"

    if is_web:
        # No push channel exists for a browser session: polling replaces
        # long-polling getUpdates, and in-app admin commands are unavailable
        # (use manage.py). See web_ingest.py for ingestion details.
        ingestor: Ingestor | WebIngestor = WebIngestor(client, settings)
        router = None
    else:
        ingestor = Ingestor(client, settings)
        router = CommandRouter(client, worker, ingestor.command_queue, settings)
    reporter = Reporter(client, settings)
    repeater = Repeater(settings)

    if hasattr(client, "get_me"):
        try:
            me = await client.get_me()
            logger.info("connected as %s", me.get("username") or me.get("id") or me)
        except EitaaError as exc:
            logger.error("getMe failed: %s", exc.message)
    else:
        logged_in = await client.is_logged_in()
        logger.info("web session logged in: %s", logged_in)
        if not logged_in:
            logger.error("no web.eitaa.com session; run `python manage.py login` first")
            await client.aclose()
            await dispose_db()
            return 1
        await client.detect_strategy()

    skipped = await worker.discard_backlog()
    if skipped:
        logger.info("dropped %d queued items/posts from while the bot was offline", skipped)

    stop_event = stop_event or asyncio.Event()
    loop = asyncio.get_running_loop()

    def request_shutdown() -> None:
        if not stop_event.is_set():
            logger.info("shutdown signal received; draining")
            stop_event.set()

    for sig_name in ("SIGTERM", "SIGINT"):
        sig = getattr(signal, sig_name, None)
        if sig is None:
            continue
        try:
            loop.add_signal_handler(sig, request_shutdown)
        except NotImplementedError:  # Windows
            signal.signal(sig, lambda *_: request_shutdown())

    tasks = [
        asyncio.create_task(ingestor.run(), name="ingest"),
        asyncio.create_task(worker.dispatcher_loop(), name="dispatcher"),
        asyncio.create_task(worker.sender_loop(), name="sender"),
        asyncio.create_task(reporter.run(), name="reporter"),
        asyncio.create_task(repeater.run(), name="repeater"),
    ]
    if router is not None:
        tasks.append(asyncio.create_task(router.run(), name="commands"))
    # RepeatItem rows come from two independent producers — the idle repeater
    # and the per-post context bundle — so this sender must run whenever
    # either one is on, not just when repeat_enabled is.
    if is_web and (settings.repeat_enabled or settings.bundle_enabled):
        tasks.append(asyncio.create_task(worker.repeat_sender_loop(), name="repeat-sender"))
    tasks.append(asyncio.create_task(_shutdown_at_window_end(stop_event, settings), name="window-end"))

    fatal_code = 0

    def _on_task_done(task: asyncio.Task) -> None:
        nonlocal fatal_code
        if task.cancelled() or stop_event.is_set():
            return
        exc = task.exception()
        if isinstance(exc, SessionUnrecoverable):
            logger.error("session unrestorable in task %s; requesting shutdown", task.get_name())
            fatal_code = 1
            stop_event.set()

    for task in tasks:
        task.add_done_callback(_on_task_done)

    await notify_admins(client, "🤖 ربات بازنشر ایتا راه‌اندازی شد.")
    logger.info("bot started")

    await stop_event.wait()

    for component in (ingestor, worker, router, reporter, repeater):
        if component is not None:
            component.stop()

    done, pending = await asyncio.wait(tasks, timeout=20)
    for task in pending:
        task.cancel()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)
    for task in done:
        exc = task.exception()
        if exc is not None and not isinstance(exc, asyncio.CancelledError):
            logger.error("task %s ended with error: %s", task.get_name(), exc)

    # Anything still 'sending' goes back to 'pending' so nothing is lost.
    await worker.requeue_stuck_items()
    await client.aclose()
    await dispose_db()
    logger.info("shutdown complete")
    return fatal_code


async def _shutdown_at_window_end(stop_event: asyncio.Event, settings) -> None:
    while not stop_event.is_set():
        remaining = seconds_until_active_window_end(settings)
        if remaining <= 0 or not is_within_active_window(settings):
            logger.info("active window ended; shutting down")
            stop_event.set()
            return
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=min(remaining, 30.0))
        except asyncio.TimeoutError:
            continue


def main() -> int:
    ensure_utf8_console()
    try:
        return asyncio.run(run())
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    sys.exit(main())
