"""Selenium transport that drives a logged-in web.eitaa.com session on Windows.

Why this exists: the eitaayar HTTP API can only send, and it cannot be added to
arbitrary groups. Driving the web client with a real user account is the only
known way to post into many groups, so this module implements the same
:class:`transport.Transport` contract the HTTP client implements. Everything
above it — queue, rate limiting, retries, reporting, CLI — is unchanged.

Two send strategies:

* ``js``  - call the web app's own message manager directly from the page
  context. Fast and far less brittle than clicking. web.eitaa.com is a fork of
  Telegram Web K, and the AutoEitaa project drives it exactly this way with
  ``appMessagesManager.sendText(chat_id, text)``.
* ``ui``  - focus the composer and type, for when the JS object is not exposed
  by the current build.

Which one works must be confirmed against a logged-in session with
``python manage.py probe``; :meth:`WebTransport.detect_strategy` does that
automatically at startup and records the result. Selectors live in SELECTORS
and can be overridden from ``selectors.json`` without touching code, because a
web UI changes on someone else's schedule, not ours.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Any, Callable, Sequence, TypeVar

from config import Settings, get_settings
from eitaa_client import EitaaError

logger = logging.getLogger("eitaa.web")


def _numeric_peer_id(value: Any, what: str) -> int:
    """Peer ids must be numeric here: the web client forwards by peer id, and
    an @username was never resolved to one. Fail as a permanent error instead
    of letting int() raise a bare ValueError that the queue reports as an
    "unexpected" fault and keeps retrying."""
    raw = str(value).strip()
    try:
        return int(raw.lstrip("@"))
    except ValueError as exc:
        raise EitaaError(
            f"{what} {raw!r} is not a numeric peer id; "
            "resolve it to its numeric id (see manage.py) and update the group",
            permanent=True,
        ) from exc



class SessionUnrecoverable(RuntimeError):
    """Browser session could not be restored; the process must exit nonzero."""

T = TypeVar("T")

# Telegram Web K style DOM, which web.eitaa.com is built on. Confirm each of
# these against the live app with `manage.py probe` before the first real run.
SELECTORS: dict[str, str] = {
    # Confirmed against the live web.eitaa.com DOM: both containers always
    # exist, so "logged in" is decided by visibility, not presence.
    "app_root": "#page-chats",
    "auth_page": "#auth-pages",
    "composer": "div.input-message-input",
    "send_button": "button.btn-send",
    "file_input": "input[type='file']",
    "attach_confirm": "button.btn-primary",
    "chat_list_item": "li.chatlist-chat",
}

SELECTORS_FILE = Path(__file__).resolve().parent / "selectors.json"

# Candidate names for the page-global message manager, most likely first.
JS_MANAGER_CANDIDATES: tuple[str, ...] = (
    "appMessagesManager",
    "window.appMessagesManager",
    "window.appImManager?.messagesManager",
)


def load_selectors() -> dict[str, str]:
    selectors = dict(SELECTORS)
    if SELECTORS_FILE.exists():
        try:
            overrides = json.loads(SELECTORS_FILE.read_text(encoding="utf-8"))
            selectors.update({str(k): str(v) for k, v in overrides.items()})
            logger.info("loaded %d selector overrides", len(overrides))
        except (ValueError, OSError) as exc:
            logger.warning("could not read %s: %s", SELECTORS_FILE, exc)
    return selectors


class WebTransport:
    """Drives one Chrome window that stays logged into web.eitaa.com."""

    BASE_URL = "https://web.eitaa.com/"

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self.selectors = load_selectors()
        self.strategy: str | None = None
        self._driver: Any = None
        self._lock = asyncio.Lock()
        # Recovery is serialised separately from driver access: three loops
        # call ensure_session, and without this they all tore down and
        # rebuilt the same browser at once, each undoing the others'
        # progress -- the log showed "session restored" and "session lost"
        # in the same second.
        self._recovery_lock = asyncio.Lock()
        self._js_manager = "appMessagesManager"

    # The queue asks this to decide whether to try album sends; the web client
    # has no album endpoint we drive, so posts go out as ordered single sends.
    @property
    def is_botapi(self) -> bool:
        return False

    # ------------------------------------------------------------------ browser

    def _build_driver(self, *, headless: bool) -> Any:
        from selenium import webdriver

        # Edge is preinstalled on Windows and its driver host is reachable from
        # Iran, while the Chrome driver download is geo-blocked. Both are
        # Chromium, so the page behaves identically.
        if self.settings.browser == "edge":
            from selenium.webdriver.edge.options import Options
        else:
            from selenium.webdriver.chrome.options import Options

        options = Options()
        profile_dir = Path(self.settings.chrome_profile_dir).resolve()
        profile_dir.mkdir(parents=True, exist_ok=True)
        options.add_argument(f"--user-data-dir={profile_dir}")
        options.add_argument("--profile-directory=Default")
        options.add_argument("--disable-notifications")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-first-run")
        options.add_argument("--no-default-browser-check")
        options.add_argument(f"--window-size={self.settings.chrome_window_size}")
        if headless:
            options.add_argument("--headless=new")
        if self.settings.chrome_binary:
            options.binary_location = str(self.settings.chrome_binary)

        if self.settings.browser == "edge":
            if self.settings.chromedriver_path:
                from selenium.webdriver.edge.service import Service

                return webdriver.Edge(
                    options=options,
                    service=Service(executable_path=str(self.settings.chromedriver_path)),
                )
            return webdriver.Edge(options=options)

        if self.settings.chromedriver_path:
            from selenium.webdriver.chrome.service import Service

            service = Service(executable_path=str(self.settings.chromedriver_path))
            return webdriver.Chrome(options=options, service=service)
        return webdriver.Chrome(options=options)

    async def _run(
        self,
        func: Callable[[], T],
        *,
        op: str = "browser",
        chat_id: str | None = None,
    ) -> T:
        """Serialise every browser interaction onto a worker thread."""
        async with self._lock:
            def _wrapped() -> T:
                if self._driver is not None:
                    self._driver.set_script_timeout(60)
                started = time.monotonic()
                try:
                    return func()
                except Exception as exc:
                    elapsed = time.monotonic() - started
                    if "script timeout" in str(exc).lower():
                        logger.error(
                            "script timeout in %s after %.1fs chat_id=%s: %s",
                            op,
                            elapsed,
                            chat_id,
                            exc,
                        )
                    raise

            return await asyncio.to_thread(_wrapped)

    def _adopt_window(self, driver: Any) -> None:
        handles = list(driver.window_handles or [])
        if not handles:
            raise EitaaError("browser window closed during startup", permanent=True)
        driver.switch_to.window(handles[-1])

    async def start(self, *, headless: bool | None = None) -> None:
        if self._driver is not None:
            return
        headless = self.settings.web_headless if headless is None else headless

        def _start() -> Any:
            driver = self._build_driver(headless=headless)
            self._adopt_window(driver)
            driver.set_script_timeout(60)
            driver.get(self.BASE_URL)
            deadline = time.monotonic() + max(self.settings.web_page_load_seconds, 8.0)
            while time.monotonic() < deadline:
                self._adopt_window(driver)
                ready = driver.execute_script("return document.readyState")
                if ready == "complete":
                    break
                time.sleep(0.5)
            else:
                time.sleep(1.0)
            return driver

        self._driver = await self._run(_start)
        logger.info("%s started (headless=%s)", self.settings.browser, headless)
        deadline = time.monotonic() + 20.0
        while time.monotonic() < deadline:
            if await self.is_logged_in():
                break
            await asyncio.sleep(1.0)

    async def aclose(self) -> None:
        driver, self._driver = self._driver, None
        if driver is None:
            return

        def _quit() -> None:
            try:
                driver.quit()
            except Exception:  # a dead browser must not block shutdown
                logger.warning("chrome did not close cleanly", exc_info=True)

        await asyncio.to_thread(_quit)

    def _require_driver(self) -> Any:
        if self._driver is None:
            raise EitaaError("browser is not started", permanent=True)
        return self._driver

    # ---------------------------------------------------------------- inspection

    async def is_logged_in(self) -> bool:
        driver = self._require_driver()
        app_root = self.selectors["app_root"]
        chat_item = self.selectors["chat_list_item"]

        def _check() -> bool:
            script = (
                "const root = document.querySelector(arguments[0]);"
                "const visible = !!root && root.offsetParent !== null;"
                "const chats = document.querySelectorAll(arguments[1]).length;"
                "return visible || chats > 0;"
            )
            try:
                return bool(driver.execute_script(script, app_root, chat_item))
            except Exception:
                return False

        return await self._run(_check)

    async def verify_api_session(self) -> tuple[bool, str | None]:
        """Ask the server whether this session's auth key is still valid.

        :meth:`is_logged_in` only looks at the DOM, and the web client renders
        its chat list straight out of IndexedDB — so a session the server has
        already invalidated still paints a full, convincing UI. Relying on the
        DOM alone means the bot logs "logged in: True", starts every loop, and
        only then discovers that each API call comes back 401 INVALID_LOGIN.

        Returns (ok, error_type); error_type is the Telegram error string
        (INVALID_LOGIN, AUTH_KEY_UNREGISTERED, SESSION_REVOKED, ...) or None.
        """
        driver = self._require_driver()

        def _check() -> Any:
            driver.set_script_timeout(45)
            script = """
            const cb = arguments[arguments.length - 1];
            try {
              apiManager.invokeApi('users.getUsers', {id: [{_: 'inputUserSelf'}]})
                .then(function (r) { cb({ok: !!(r && r[0])}); })
                .catch(function (e) {
                  cb({ok: false, type: (e && (e.type || e.error_message)) || 'UNKNOWN'});
                });
            } catch (e) { cb({ok: false, type: 'NO_API_MANAGER'}); }
            """
            return driver.execute_async_script(script)

        try:
            result = await self._run(_check, op="verify_session")
        except Exception as exc:
            logger.warning("session verification could not run: %s", exc)
            return False, "VERIFY_FAILED"
        if isinstance(result, dict) and result.get("ok"):
            return True, None
        error_type = (result or {}).get("type") if isinstance(result, dict) else "UNKNOWN"
        return False, error_type

    async def _healthy(self) -> bool:
        if self._driver is None:
            return False
        try:
            return await self.is_logged_in()
        except Exception:
            logger.exception("session health check failed")
            return False

    async def ensure_session(self) -> None:
        """Restart Edge with the on-disk profile if the session dies mid-run.

        Three retries, 30/60/120s apart. After that the process must exit
        nonzero so NSSM / Task Scheduler can bring it back.
        """
        if await self._healthy():
            return

        async with self._recovery_lock:
            # Another loop may have rebuilt the browser while this one waited.
            if await self._healthy():
                return
            await self._restart_session()

    async def _restart_session(self) -> None:
        delays = (30.0, 60.0, 120.0)
        for attempt, delay in enumerate(delays, start=1):
            logger.error(
                "web session lost; restart attempt %s/3 after %.0fs",
                attempt,
                delay,
            )
            await asyncio.sleep(delay)
            try:
                await self.aclose()
            except Exception:
                logger.exception("error closing dead browser")
            self._driver = None
            self.strategy = None
            try:
                await self.start()
                if await self._healthy():
                    await self.detect_strategy()
                    logger.info("web session restored on attempt %s", attempt)
                    try:
                        from reporter import notify_admins

                        await notify_admins(
                            self,
                            "⚠️ نشست مرورگر ایتا از دست رفت و دوباره برقرار شد.",
                        )
                    except Exception:
                        logger.exception("could not notify admins after session restore")
                    return
            except Exception:
                logger.exception("browser restart attempt %s failed", attempt)

        logger.error("web session unrestorable after 3 attempts; stopping")
        raise SessionUnrecoverable("web session unrestorable after 3 retries")

    async def detect_strategy(self) -> str:
        """Pick the js fast path when the page exposes a message manager."""
        driver = self._require_driver()

        def _detect() -> str:
            for candidate in JS_MANAGER_CANDIDATES:
                try:
                    found = driver.execute_script(
                        f"try {{ return typeof ({candidate}) !== 'undefined' "
                        f"&& typeof ({candidate}).sendText === 'function'; }} "
                        f"catch (e) {{ return false; }}"
                    )
                except Exception:
                    found = False
                if found:
                    self._js_manager = candidate
                    return "js"
            return "ui"

        self.strategy = await self._run(_detect)
        logger.info("web transport strategy: %s", self.strategy)
        return self.strategy

    async def probe(self) -> dict[str, Any]:
        """Report what actually resolves in the live DOM, for first-run setup."""
        from selenium.webdriver.common.by import By

        driver = self._require_driver()

        def _probe() -> dict[str, Any]:
            found: dict[str, Any] = {}
            for name, selector in self.selectors.items():
                try:
                    found[name] = len(driver.find_elements(By.CSS_SELECTOR, selector))
                except Exception as exc:
                    found[name] = f"error: {exc}"
            return found

        selectors_state = await self._run(_probe)
        return {
            "url": await self._run(lambda: self._require_driver().current_url),
            "logged_in": await self.is_logged_in(),
            "strategy": await self.detect_strategy(),
            "selectors": selectors_state,
        }

    # -------------------------------------------------------------------- sending

    async def _open_chat(self, chat_id: str) -> None:
        driver = self._require_driver()
        target = chat_id.strip()

        def _open() -> None:
            # Invite links open the join/preview view; numeric peer ids and
            # usernames address an existing dialog directly.
            if target.startswith("http"):
                driver.get(target)
            else:
                driver.get(f"{self.BASE_URL}#{target.lstrip('@')}")
            time.sleep(self.settings.web_chat_open_seconds)

        await self._run(_open)

    async def send_message(
        self, chat_id: str, text: str, *, disable_notification: bool = False
    ) -> Any:
        await self.ensure_session()
        if self.strategy is None:
            await self.detect_strategy()

        if self.strategy == "js":
            return await self._send_text_js(chat_id, text)
        return await self._send_text_ui(chat_id, text)

    async def _send_text_js(self, chat_id: str, text: str) -> Any:
        driver = self._require_driver()
        manager = getattr(self, "_js_manager", "appMessagesManager")

        def _send() -> Any:
            payload = json.dumps({"peer": chat_id, "text": text})
            script = (
                "const a = arguments[0];"
                f"const m = {manager};"
                "const peer = isNaN(Number(a.peer)) ? a.peer : Number(a.peer);"
                "m.sendText(peer, a.text);"
                "return true;"
            )
            return driver.execute_script(script, json.loads(payload))

        try:
            result = await self._run(_send, op="send_text", chat_id=chat_id)
        except Exception as exc:
            raise EitaaError(f"js sendText failed: {exc}") from exc
        if not result:
            raise EitaaError("js sendText returned falsy")
        return {"ok": True, "strategy": "js"}

    async def _send_text_ui(self, chat_id: str, text: str) -> Any:
        from selenium.webdriver.common.by import By
        from selenium.webdriver.common.keys import Keys

        await self._open_chat(chat_id)
        driver = self._require_driver()

        def _send() -> Any:
            boxes = driver.find_elements(By.CSS_SELECTOR, self.selectors["composer"])
            if not boxes:
                raise EitaaError(
                    f"composer not found for {chat_id}; the bot may not be a member",
                    permanent=True,
                )
            box = boxes[-1]
            box.click()
            for line in text.split("\n"):
                box.send_keys(line)
                box.send_keys(Keys.SHIFT, Keys.ENTER)
            box.send_keys(Keys.ENTER)
            time.sleep(self.settings.web_send_settle_seconds)
            return {"ok": True, "strategy": "ui"}

        try:
            return await self._run(_send, op="send_text_ui", chat_id=chat_id)
        except EitaaError:
            raise
        except Exception as exc:
            raise EitaaError(f"ui send failed for {chat_id}: {exc}") from exc

    async def send_media(
        self,
        chat_id: str,
        media_type: str,
        *,
        file_id: str | None = None,
        local_path: str | None = None,
        caption: str | None = None,
        file_name: str | None = None,
        disable_notification: bool = False,
    ) -> Any:
        from selenium.webdriver.common.by import By
        from selenium.webdriver.common.keys import Keys

        await self.ensure_session()
        if not local_path:
            # file_id belongs to the HTTP bot API and means nothing to the web app.
            raise EitaaError("web transport needs a local file", permanent=True)
        path = Path(local_path).resolve()
        if not path.exists():
            raise EitaaError(f"file not found: {path}", permanent=True)

        await self._open_chat(chat_id)
        driver = self._require_driver()

        def _send() -> Any:
            inputs = driver.find_elements(By.CSS_SELECTOR, self.selectors["file_input"])
            if not inputs:
                raise EitaaError("file input not found in the web client", permanent=True)
            inputs[-1].send_keys(str(path))
            time.sleep(self.settings.web_attach_settle_seconds)

            if caption:
                boxes = driver.find_elements(By.CSS_SELECTOR, self.selectors["composer"])
                if boxes:
                    boxes[-1].click()
                    boxes[-1].send_keys(caption)

            buttons = driver.find_elements(By.CSS_SELECTOR, self.selectors["attach_confirm"])
            if buttons:
                buttons[-1].click()
            else:
                driver.switch_to.active_element.send_keys(Keys.ENTER)
            time.sleep(self.settings.web_send_settle_seconds)
            return {"ok": True, "strategy": "ui-file"}

        try:
            return await self._run(_send, op="send_media", chat_id=chat_id)
        except EitaaError:
            raise
        except Exception as exc:
            raise EitaaError(f"file send failed for {chat_id}: {exc}") from exc

    async def send_media_group(
        self, chat_id: str, items: Sequence[dict[str, Any]], *, disable_notification: bool = False
    ) -> Any:
        raise EitaaError("web transport has no album endpoint", permanent=True)

    # --------------------------------------------------------- invite resolving

    async def resolve_invite_hashes(self, hashes: Sequence[str]) -> dict[str, Any]:
        """Turn eitaa.com/joinchat hashes into numeric peer ids.

        Everything downstream forwards by numeric peer id, and an invite link
        carries none. For a chat the account has already joined the server
        answers `chatInviteAlready` with the chat itself, which is where the id
        comes from; resolving also teaches the web client about the peer, so
        groups that previously answered PEER_ID_INVALID can become reachable.

        Returns {hash: {"id": int, "title": str}} for what resolved and
        {hash: {"error": str}} for what did not, so the caller can report the
        bad links rather than silently dropping them.
        """
        await self.ensure_session()
        driver = self._require_driver()

        def _resolve() -> Any:
            driver.set_script_timeout(300)
            script = """
            const cb = arguments[arguments.length - 1];
            const hashes = arguments[0];
            (async () => {
              const out = {};
              for (const h of hashes) {
                try {
                  const r = await apiManager.invokeApi('messages.checkChatInvite', {hash: h});
                  if ((r._ === 'chatInviteAlready' || r._ === 'chatInvitePeek') && r.chat) {
                    out[h] = {id: r.chat.id, title: r.chat.title || '', joined: true};
                  } else {
                    // A plain chatInvite means the account is not in that chat.
                    out[h] = {error: 'not a member', title: r.title || ''};
                  }
                } catch (e) {
                  out[h] = {error: (e && (e.type || e.error_message)) || String(e)};
                }
                await new Promise(r => setTimeout(r, 120));  // stay polite
              }
              cb(out);
            })();
            """
            return driver.execute_async_script(script, list(hashes))

        result = await self._run(_resolve, op="resolve_invites")
        return result if isinstance(result, dict) else {}

    # ----------------------------------------------------------- channel reads

    async def get_channel_message_ids(
        self,
        peer_id: str,
        *,
        before_id: int | None = None,
        skip: int = 0,
        limit: int = 10,
    ) -> list[int]:
        """The channel's own recent message ids, newest first.

        The bot's `posts` table is a poor stand-in for channel history: it only
        holds what was ingested while the bot happened to be running, and every
        restart skips whatever was published during the downtime. After a few
        outages it held 87 rows against a channel that had published 17,569
        messages, so "the 9 posts before this one" resolved to messages that
        were sixty and three hundred places back in the real channel. Anything
        that means "recent posts" has to ask the channel.

        `before_id` returns only messages older than that id, `skip` walks
        further back in blocks (round 1 wants 10..20), and empty or service
        messages are dropped so a batch of 10 is 10 real posts.
        """
        await self.ensure_session()
        driver = self._require_driver()
        peer = _numeric_peer_id(peer_id, "source channel")

        def _fetch() -> Any:
            script = """
            const cb = arguments[arguments.length - 1];
            const peer = arguments[0], beforeId = arguments[1];
            const skip = arguments[2], want = arguments[3];
            (async () => {
              try {
                const out = [];
                let offsetId = beforeId || 0;
                let guard = 0;
                // Over-fetch: service and empty messages do not count towards
                // the batch, and skipping blocks needs the ones before them.
                while (out.length < skip + want && guard++ < 12) {
                  const r = await apiManager.invokeApi('messages.getHistory', {
                    peer: appPeersManager.getInputPeerById(peer),
                    offset_id: offsetId, offset_date: 0, add_offset: 0,
                    limit: 100, max_id: 0, min_id: 0, hash: 0});
                  const msgs = r.messages || [];
                  if (!msgs.length) break;
                  for (const m of msgs) {
                    if (m._ === 'messageEmpty' || m._ === 'messageService') continue;
                    if (!m.message && !m.media) continue;
                    out.push(m.id);
                  }
                  offsetId = msgs[msgs.length - 1].id;
                }
                cb({ok: true, ids: out.slice(skip, skip + want)});
              } catch (e) {
                cb({ok: false, error: (e && (e.type || e.error_message || e.message)) || String(e)});
              }
            })();
            """
            return driver.execute_async_script(script, peer, int(before_id or 0), int(skip), int(limit))

        result = await self._run(_fetch, op="channel_history")
        if not isinstance(result, dict) or not result.get("ok"):
            error = result.get("error") if isinstance(result, dict) else str(result)
            raise EitaaError(f"reading channel history failed: {error}")
        return [int(i) for i in (result.get("ids") or [])]

    # ------------------------------------------------------------------- forward

    async def forward_message(
        self, from_peer_id: str, message_ids: Sequence[int], to_peer_id: str
    ) -> Any:
        """Forward every id in one server call, preserving albums.

        Goes straight at ``messages.forwardMessages`` through ``apiManager``
        rather than the web client's ``appMessagesManager.forwardMessages``
        wrapper, for two reasons found the hard way:

        * The wrapper forwards out of the client's local message cache and
          takes its *internal* mids, not server message ids. Ingest reads
          history through ``apiManager``, which never fills that cache, so the
          wrapper looked up nothing and died inside the client with "Cannot
          read properties of undefined (reading 'isUser')". The raw call takes
          the server ids we already store, so none of that applies.
        * The wrapper resolves once the outgoing message is queued locally,
          which is not proof the server took it. The raw call returns the
          server's own Updates, so a success here means the message exists.

        Passing every id in one call is what keeps an album together; sending
        them one by one would split it into separate messages.
        """
        await self.ensure_session()
        driver = self._require_driver()
        ids = [int(i) for i in message_ids]
        from_id = _numeric_peer_id(from_peer_id, "source channel")
        to_id = _numeric_peer_id(to_peer_id, "destination group")

        def _forward() -> Any:
            script = """
            const fromPeer = arguments[0];
            const toPeer = arguments[1];
            const ids = arguments[2];
            const done = arguments[3];
            (async () => {
              try {
                // A destination the client has not loaded yet builds a bogus
                // input peer and the server answers PEER_ID_INVALID, which is
                // indistinguishable from never having joined. Load the dialog
                // list once so the peer is known before deciding anything.
                let known = null;
                try { known = appPeersManager.getPeer(toPeer); } catch (e) {}
                if (!known || !known._ || known._ === 'chatEmpty') {
                  for (const folder of [0, 1]) {
                    try {
                      await apiManager.invokeApi('messages.getDialogs', {
                        offset_date: 0, offset_id: 0, offset_peer: {_: 'inputPeerEmpty'},
                        limit: 100, hash: 0, folder_id: folder});
                    } catch (e) { /* the send below is the real test */ }
                  }
                }
                // random_id must be unique per message or the server treats a
                // repeat as a duplicate send and silently drops it.
                const randomIds = ids.map(() => {
                  if (typeof randomLong === 'function') return randomLong();
                  return String(Date.now()) + String(Math.floor(Math.random() * 1e9));
                });
                const res = await apiManager.invokeApi('messages.forwardMessages', {
                  from_peer: appPeersManager.getInputPeerById(fromPeer),
                  id: ids.slice(),
                  random_id: randomIds,
                  to_peer: appPeersManager.getInputPeerById(toPeer),
                  silent: false,
                  background: false,
                  drop_author: false,
                  drop_media_captions: false
                });
                const updates = (res && res.updates) ? res.updates.map(u => u._) : [];
                // The server answers an accepted forward with the new message
                // itself; anything else means nothing was actually posted.
                const delivered = updates.some(u =>
                  u === 'updateNewMessage' || u === 'updateNewChannelMessage' ||
                  u === 'updateMessageID');
                if (!delivered) {
                  done({ok: false, error: 'server returned no new-message update: ' + updates.join(',')});
                  return;
                }
                done({ok: true, result: (res && res._) || 'ok', updates: updates});
              } catch (e) {
                done({
                  ok: false,
                  error: (e && (e.type || e.error_message || e.text || e.message)) ||
                         JSON.stringify(e, Object.getOwnPropertyNames(e || {})) || String(e)
                });
              }
            })();
            """
            return driver.execute_async_script(script, from_id, to_id, ids)

        try:
            result = await self._run(_forward, op="forward", chat_id=str(to_peer_id))
        except Exception as exc:
            raise EitaaError(f"forward failed: {exc}") from exc
        if not isinstance(result, dict) or not result.get("ok"):
            error = result.get("error") if isinstance(result, dict) else str(result)
            permanent = any(
                marker in str(error).upper()
                for marker in (
                    "CHAT_WRITE_FORBIDDEN",
                    "CHAT_FORBIDDEN",
                    "CHAT_ADMIN_REQUIRED",
                    "USER_BANNED",
                    "CHANNEL_PRIVATE",
                    # PEER_ID_INVALID is deliberately absent: the web client
                    # returns it for any peer it has not loaded yet, so
                    # treating it as permanent disabled 59 groups in one run
                    # that the server had just confirmed the account was in.
                    # It still fails after the normal retries.
                    "USER_KICKED",
                    "CHANNEL_INVALID",
                    "USER_IS_BLOCKED",
                    "MESSAGE_ID_INVALID",
                    "INPUT_USER_DEACTIVATED",
                )
            )
            raise EitaaError(f"forward failed: {error}", permanent=permanent)
        return result


async def build_transport(settings: Settings | None = None) -> Any:
    """Return the transport selected by configuration."""
    settings = settings or get_settings()
    if settings.transport == "web":
        transport = WebTransport(settings)
        await transport.start()
        return transport
    from eitaa_client import EitaaClient

    return EitaaClient(settings)
