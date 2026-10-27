"""One-off operational script: join the invite-link groups we're not yet a
member of, spaced out with randomized human-like delays.

This is deliberately NOT part of the always-on bot — joining 32 groups back
to back from one account is exactly the pattern anti-spam systems flag.
Run this manually, once, whenever the client hands over a fresh batch of
group links that still need joining. It reads resolve_result.json (produced
by the checkChatInvite resolution pass) and only acts on entries where
`already` is false.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import get_settings, setup_logging  # noqa: E402
from db import Group, init_db, session_scope  # noqa: E402
from sqlalchemy import select  # noqa: E402
from web_transport import WebTransport  # noqa: E402

logger = logging.getLogger("eitaa.join")

MIN_DELAY_SECONDS = 8
MAX_DELAY_SECONDS = 15


async def join_one(transport: WebTransport, hash_: str) -> dict:
    driver = transport._require_driver()  # noqa: SLF001

    def _join() -> dict:
        script = f"""
        return apiManager.invokeApi('messages.importChatInvite', {{hash: '{hash_}'}})
          .then(r => {{
            const chat = (r.chats && r.chats[0]) || null;
            return {{ok: true, id: chat ? chat.id : null, title: chat ? chat.title : null}};
          }})
          .catch(e => ({{ok: false, error: String(e && e.message || e)}}));
        """
        return driver.execute_script(script)

    return await transport._run(_join)  # noqa: SLF001


async def main() -> None:
    setup_logging()
    await init_db()
    settings = get_settings()

    pending_file = Path("resolve_result.json")
    if not pending_file.exists():
        logger.error("resolve_result.json not found; run the resolution pass first")
        return
    entries = json.loads(pending_file.read_text(encoding="utf-8"))
    targets = [e for e in entries if not e.get("already") and not e.get("error")]
    logger.info("%d groups to join", len(targets))

    transport = WebTransport(settings)
    await transport.start(headless=False)

    joined, failed = 0, 0
    try:
        for index, entry in enumerate(targets, start=1):
            result = await join_one(transport, entry["hash"])
            if result.get("ok") and result.get("id"):
                chat_id = str(-result["id"])
                title = result.get("title") or entry.get("title")
                async with session_scope() as session:
                    existing = (
                        await session.execute(select(Group).where(Group.chat_id == chat_id))
                    ).scalar_one_or_none()
                    if existing is None:
                        session.add(Group(chat_id=chat_id, title=title))
                joined += 1
                logger.info("[%d/%d] joined: %s (%s)", index, len(targets), title, chat_id)
            else:
                failed += 1
                logger.warning(
                    "[%d/%d] join failed for %s: %s",
                    index, len(targets), entry.get("title"), result.get("error"),
                )

            if index < len(targets):
                delay = random.uniform(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS)
                logger.info("waiting %.0fs before next join", delay)
                await asyncio.sleep(delay)
    finally:
        await transport.aclose()

    logger.info("done: joined=%d failed=%d", joined, failed)


if __name__ == "__main__":
    asyncio.run(main())
