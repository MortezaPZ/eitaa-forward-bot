from __future__ import annotations

import asyncio
import json
import logging
import mimetypes
from pathlib import Path
from typing import Any, Iterable, Sequence

import httpx

from config import Settings, get_settings

logger = logging.getLogger("eitaa.client")

# Substrings that mark an error as permanent for a given chat: retrying will
# never help, the group must be flagged and reported to the admins instead.
PERMANENT_ERROR_MARKERS: tuple[str, ...] = (
    "chat not found",
    "chat_id is empty",
    "bot was kicked",
    "bot is not a member",
    "not enough rights",
    "have no rights",
    "chat_write_forbidden",
    "user is deactivated",
    "group chat was upgraded",
    "peer_id_invalid",
    "forbidden",
    "access denied",
    "unauthorized chat",
)

MEDIA_METHODS: dict[str, str] = {
    "photo": "sendPhoto",
    "video": "sendVideo",
    "audio": "sendAudio",
    "voice": "sendVoice",
    "document": "sendDocument",
    "animation": "sendAnimation",
    "video_note": "sendVideoNote",
    "sticker": "sendSticker",
}

# Media types Eitaa/Telegram allow inside a single sendMediaGroup call.
ALBUM_TYPES: frozenset[str] = frozenset({"photo", "video", "document", "audio"})


class EitaaError(Exception):
    """Base error for every failed Eitaa API interaction."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        permanent: bool = False,
        retry_after: float | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.permanent = permanent
        self.retry_after = retry_after


class RateLimitError(EitaaError):
    """Raised on HTTP 429 / Too Many Requests; carries retry_after seconds."""

    def __init__(self, message: str, retry_after: float) -> None:
        super().__init__(message, status_code=429, permanent=False, retry_after=retry_after)


def classify_error(description: str, status_code: int | None) -> bool:
    lowered = description.lower()
    if any(marker in lowered for marker in PERMANENT_ERROR_MARKERS):
        return True
    return status_code in {400, 403} and "too many" not in lowered


class EitaaClient:
    """Async client for the Eitaa bot API.

    Two wire formats are supported:

    * ``botapi``   -> ``{base}/bot{token}/{method}``, Telegram-compatible.
    * ``eitaayar`` -> ``{base}/{token}/{method}``, the send-only eitaayar.ir
      gateway. It exposes no ``getUpdates`` and no ``forwardMessage``, and it
      collapses every attachment method into ``sendFile``; file_id values from
      one API are not reusable there, so media is uploaded from disk.
    """

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(self.settings.http_timeout_seconds),
            follow_redirects=True,
        )

    @property
    def is_botapi(self) -> bool:
        return self.settings.api_mode == "botapi"

    def _url(self, method: str) -> str:
        base = self.settings.api_base_url.rstrip("/")
        token = self.settings.bot_token
        if self.is_botapi:
            return f"{base}/bot{token}/{method}"
        return f"{base}/{token}/{method}"

    async def aclose(self) -> None:
        await self._client.aclose()

    async def request(
        self,
        method: str,
        data: dict[str, Any] | None = None,
        files: dict[str, Any] | None = None,
        *,
        timeout: float | None = None,
    ) -> Any:
        payload = {k: v for k, v in (data or {}).items() if v is not None}
        try:
            response = await self._client.post(
                self._url(method),
                data=payload,
                files=files,
                timeout=timeout or self.settings.http_timeout_seconds,
            )
        except httpx.TimeoutException as exc:
            raise EitaaError(f"timeout calling {method}: {exc}") from exc
        except httpx.HTTPError as exc:
            raise EitaaError(f"network error calling {method}: {exc}") from exc

        return self._parse(method, response)

    def _parse(self, method: str, response: httpx.Response) -> Any:
        try:
            body = response.json()
        except ValueError:
            body = {}
            if response.status_code != 429:
                # A non-JSON 200 usually means the base URL points at a web page
                # rather than the bot API; surfacing it beats silent no-ops.
                raise EitaaError(
                    f"{method}: non-JSON response ({response.status_code}) from {response.url}",
                    status_code=response.status_code,
                )

        if response.status_code == 429:
            retry_after = float(
                body.get("parameters", {}).get("retry_after")
                or response.headers.get("Retry-After")
                or 30
            )
            raise RateLimitError(f"{method}: rate limited", retry_after)

        ok = bool(body.get("ok", response.is_success))
        if ok:
            return body.get("result", body)

        description = str(body.get("description") or body.get("error") or response.text)[:500]
        retry_after = body.get("parameters", {}).get("retry_after")
        if retry_after:
            raise RateLimitError(f"{method}: {description}", float(retry_after))
        raise EitaaError(
            f"{method}: {description}",
            status_code=response.status_code,
            permanent=classify_error(description, response.status_code),
        )

    async def get_me(self) -> dict[str, Any]:
        return await self.request("getMe")

    async def get_updates(self, offset: int, limit: int = 100) -> list[dict[str, Any]]:
        if not self.is_botapi:
            # The eitaayar gateway is send-only; long polling is unavailable.
            return []
        timeout = self.settings.poll_timeout_seconds
        result = await self.request(
            "getUpdates",
            {"offset": offset, "limit": limit, "timeout": timeout},
            timeout=timeout + self.settings.http_timeout_seconds,
        )
        return list(result or [])

    async def send_message(
        self,
        chat_id: str,
        text: str,
        *,
        disable_notification: bool = False,
    ) -> dict[str, Any]:
        data: dict[str, Any] = {
            "chat_id": chat_id,
            "text": text,
            "disable_notification": 1 if disable_notification else None,
        }
        return await self.request("sendMessage", data)

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
    ) -> dict[str, Any]:
        """Send one attachment, preferring file_id and falling back to upload."""
        if self.is_botapi and file_id:
            method = MEDIA_METHODS.get(media_type, "sendDocument")
            field = media_type if media_type in MEDIA_METHODS else "document"
            try:
                return await self.request(
                    method,
                    {
                        "chat_id": chat_id,
                        field: file_id,
                        "caption": caption,
                        "disable_notification": disable_notification,
                    },
                )
            except EitaaError as exc:
                if not local_path or exc.permanent:
                    raise
                logger.warning("file_id send failed (%s); retrying as upload", exc.message)

        if not local_path or not Path(local_path).exists():
            raise EitaaError(
                f"no usable payload for {media_type} (file_id unusable and no local copy)",
                permanent=True,
            )

        method = "sendFile" if not self.is_botapi else MEDIA_METHODS.get(media_type, "sendDocument")
        field = "file" if not self.is_botapi else (
            media_type if media_type in MEDIA_METHODS else "document"
        )
        path = Path(local_path)
        mime = mimetypes.guess_type(file_name or path.name)[0] or "application/octet-stream"
        with path.open("rb") as handle:
            files = {field: (file_name or path.name, handle, mime)}
            return await self.request(
                method,
                {
                    "chat_id": chat_id,
                    "caption": caption,
                    "disable_notification": disable_notification,
                },
                files=files,
            )

    async def send_media_group(
        self,
        chat_id: str,
        items: Sequence[dict[str, Any]],
        *,
        disable_notification: bool = False,
    ) -> Any:
        """Send an album as one unit, preserving the original item order.

        Eitaa has no forwardMessage/copyMessage that keeps album grouping, so
        the album is re-uploaded as a fresh media group. Callers fall back to
        sequential sends when the gateway rejects the method.
        """
        if not self.is_botapi:
            raise EitaaError("sendMediaGroup unsupported in eitaayar mode", permanent=True)

        media: list[dict[str, Any]] = []
        files: dict[str, Any] = {}
        handles: list[Any] = []
        try:
            for index, item in enumerate(items):
                media_type = item["media_type"]
                if media_type not in ALBUM_TYPES:
                    raise EitaaError(f"{media_type} cannot be part of an album", permanent=True)
                entry: dict[str, Any] = {"type": media_type}
                if item.get("caption"):
                    entry["caption"] = item["caption"]
                if item.get("file_id"):
                    entry["media"] = item["file_id"]
                else:
                    local_path = item.get("local_path")
                    if not local_path or not Path(local_path).exists():
                        raise EitaaError("album item has no payload", permanent=True)
                    key = f"file{index}"
                    path = Path(local_path)
                    handle = path.open("rb")
                    handles.append(handle)
                    mime = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
                    files[key] = (item.get("file_name") or path.name, handle, mime)
                    entry["media"] = f"attach://{key}"
                media.append(entry)

            return await self.request(
                "sendMediaGroup",
                {
                    "chat_id": chat_id,
                    "media": json.dumps(media, ensure_ascii=False),
                    "disable_notification": disable_notification,
                },
                files=files or None,
            )
        finally:
            for handle in handles:
                handle.close()

    async def get_chat(self, chat_id: str) -> dict[str, Any]:
        return await self.request("getChat", {"chat_id": chat_id})

    async def download_file(self, file_id: str, destination: Path) -> Path | None:
        """Cache a file locally so albums and retries never depend on file_id."""
        if not self.is_botapi:
            return None
        try:
            info = await self.request("getFile", {"file_id": file_id})
            file_path = info.get("file_path")
            if not file_path:
                return None
            base = self.settings.api_base_url.rstrip("/")
            url = f"{base}/file/bot{self.settings.bot_token}/{file_path}"
            destination.parent.mkdir(parents=True, exist_ok=True)
            async with self._client.stream("GET", url) as response:
                if response.status_code != 200:
                    return None
                with destination.open("wb") as handle:
                    async for chunk in response.aiter_bytes():
                        handle.write(chunk)
            return destination
        except (EitaaError, OSError, httpx.HTTPError) as exc:
            logger.warning("could not cache file %s: %s", file_id, exc)
            return None


async def gather_close(clients: Iterable[EitaaClient]) -> None:
    await asyncio.gather(*(client.aclose() for client in clients), return_exceptions=True)
