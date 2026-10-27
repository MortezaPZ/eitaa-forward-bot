"""The seam between the delivery engine and however messages actually get sent.

`QueueWorker` only ever touches the four members below. `EitaaClient` (HTTP)
satisfies this protocol today; a second implementation — e.g. one driving a
logged-in web.eitaa.com session — can be dropped in without touching the queue,
rate limiting, retry, reporting or CLI code.
"""

from __future__ import annotations

from typing import Any, Protocol, Sequence, runtime_checkable


@runtime_checkable
class Transport(Protocol):
    @property
    def is_botapi(self) -> bool:
        """True when the backend supports albums and inbound updates."""

    async def send_message(
        self, chat_id: str, text: str, *, disable_notification: bool = False
    ) -> Any: ...

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
    ) -> Any: ...

    async def send_media_group(
        self, chat_id: str, items: Sequence[dict[str, Any]], *, disable_notification: bool = False
    ) -> Any: ...

    async def aclose(self) -> None: ...
