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

    # ------------------------------------------------------------------- forward

    async def forward_message(
        self, from_peer_id: str, message_ids: Sequence[int], to_peer_id: str
    ) -> Any:
        """Native forward of every id in one call, preserving albums.

        Live web.eitaa.com (2026-08-20) exposes
        ``appMessagesManager.forwardMessages(toPeer, fromPeer, ids)``. That
        function sorts the ids, keeps same-``grouped_id`` parts together, and
        then invokes ``messages.forwardMessages`` once. Sending ids one-by-one
        would break the album; this path never does that.
        """
        await self.ensure_session()
        driver = self._require_driver()
        ids = [int(i) for i in message_ids]
        from_id = int(str(from_peer_id).lstrip("@"))
        to_id = int(str(to_peer_id).lstrip("@"))

        def _forward() -> Any:
            script = """
            const toPeer = arguments[0];
            const fromPeer = arguments[1];
            const ids = arguments[2];
            const done = arguments[3];
            try {
              const pending = appMessagesManager.forwardMessages(toPeer, fromPeer, ids.slice());
              Promise.resolve(pending)
                .then(r => done({ok: true, result: (r && r._) || 'ok'}))
                .catch(e => done({
                  ok: false,
                  error: (e && (e.text || e.type || e.message)) || JSON.stringify(e) || String(e)
                }));
            } catch (e) {
              done({ok: false, error: String(e && e.message || e)});
            }
            """
            return driver.execute_async_script(script, to_id, from_id, ids)

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
                    "PEER_ID_INVALID",
                    "USER_KICKED",
                    "CHANNEL_INVALID",
                    "USER_IS_BLOCKED",
                    "CHAT_ID_INVALID",
                    "USER_NOT_PARTICIPANT",
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
