"""Local FastAPI panel for managing destination groups.

Runs as its own process (see WINDOWS_DEPLOY.md, EitaaPanel). Bind only to a
private address — this uses HTTP Basic Auth without HTTPS and must not be
exposed on the public internet.
"""

from __future__ import annotations

import secrets
from pathlib import Path

from fastapi import Depends, FastAPI, Form, HTTPException, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.templating import Jinja2Templates
from sqlalchemy import select

from commands import add_managed_group, remove_managed_group, set_managed_group_enabled
from config import get_settings, setup_logging, ensure_utf8_console
from db import DEFAULTS, Group, init_db, session_scope, settings_store

TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
security = HTTPBasic()
app = FastAPI(title="Eitaa group panel", docs_url=None, redoc_url=None)


def _require_user(credentials: HTTPBasicCredentials = Depends(security)) -> str:
    settings = get_settings()
    if not settings.panel_username or not settings.panel_password:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="PANEL_USERNAME and PANEL_PASSWORD must be set in .env",
        )
    user_ok = secrets.compare_digest(credentials.username, settings.panel_username)
    pass_ok = secrets.compare_digest(credentials.password, settings.panel_password)
    if not (user_ok and pass_ok):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid credentials",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username


@app.on_event("startup")
async def _startup() -> None:
    setup_logging()
    await init_db()
    await settings_store.load()
    for key, default in DEFAULTS.items():
        if not await settings_store.has(key):
            await settings_store.set(key, default)


@app.get("/", response_class=HTMLResponse)
async def list_groups(request: Request, _: str = Depends(_require_user)) -> HTMLResponse:
    paused = await settings_store.get_bool("paused")
    async with session_scope() as session:
        groups = (await session.execute(select(Group).order_by(Group.id))).scalars().all()
        total = len(groups)
        enabled = sum(1 for g in groups if g.enabled)
        rows = [
            {
                "id": g.id,
                "chat_id": g.chat_id,
                "title": g.title or "",
                "status": g.status,
                "enabled": g.enabled,
                "fail_count": g.fail_count,
                "last_sent_at": g.last_sent_at,
            }
            for g in groups
        ]
    return templates.TemplateResponse(
        request,
        "groups.html",
        {
            "groups": rows,
            "total": total,
            "enabled_count": enabled,
            "paused": paused,
            "flash": request.query_params.get("msg"),
        },
    )


@app.post("/groups/add")
async def add_group(raw: str = Form(...), _: str = Depends(_require_user)) -> RedirectResponse:
    _code, message = await add_managed_group(raw, None)
    return RedirectResponse(f"/?msg={message}", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/groups/delete")
async def delete_group(chat_id: str = Form(...), _: str = Depends(_require_user)) -> RedirectResponse:
    _code, message = await remove_managed_group(chat_id)
    return RedirectResponse(f"/?msg={message}", status_code=status.HTTP_303_SEE_OTHER)


@app.post("/groups/toggle")
async def toggle_group(chat_id: str = Form(...), _: str = Depends(_require_user)) -> RedirectResponse:
    async with session_scope() as session:
        group = (
            await session.execute(select(Group).where(Group.chat_id == chat_id))
        ).scalar_one_or_none()
        currently_enabled = bool(group.enabled) if group is not None else False
    _code, message = await set_managed_group_enabled(chat_id, not currently_enabled)
    return RedirectResponse(f"/?msg={message}", status_code=status.HTTP_303_SEE_OTHER)


def main() -> None:
    import uvicorn

    ensure_utf8_console()
    settings = get_settings()
    uvicorn.run(
        "web_panel:app",
        host=settings.panel_host,
        port=settings.panel_port,
        reload=False,
    )


if __name__ == "__main__":
    main()
