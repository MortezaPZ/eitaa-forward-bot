from __future__ import annotations

import datetime as dt
import logging
import logging.handlers
import sys
from functools import lru_cache
from pathlib import Path
from typing import Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = Path(__file__).resolve().parent


def ensure_utf8_console() -> None:
    """Windows cp1256 cannot print Persian; force UTF-8 on stdout/stderr."""
    for _stream in (sys.stdout, sys.stderr):
        if hasattr(_stream, "reconfigure"):
            _stream.reconfigure(encoding="utf-8", errors="replace")


class Settings(BaseSettings):
    """Runtime configuration loaded from environment / .env file."""

    model_config = SettingsConfigDict(
        env_file=BASE_DIR / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    bot_token: str = Field(alias="EITAA_BOT_TOKEN")
    api_base_url: str = Field(default="https://eitaa.com/api", alias="EITAA_API_BASE_URL")
    api_mode: Literal["botapi", "eitaayar"] = Field(default="botapi", alias="EITAA_API_MODE")

    source_channel_id: str = Field(alias="SOURCE_CHANNEL_ID")
    admin_ids_raw: str = Field(default="", alias="ADMIN_IDS")

    database_url: str = Field(default="sqlite+aiosqlite:///./data/bot.db", alias="DATABASE_URL")
    files_dir: Path = Field(default=BASE_DIR / "data" / "files", alias="FILES_DIR")

    send_delay_seconds: int = Field(default=60, alias="SEND_DELAY_SECONDS")
    send_interval_seconds: float = Field(default=1.0, alias="SEND_INTERVAL_SECONDS")
    send_jitter_ratio: float = Field(default=0.35, alias="SEND_JITTER_RATIO")
    max_sends_per_minute: int = Field(default=45, alias="MAX_SENDS_PER_MINUTE")

    max_retries: int = Field(default=3, alias="MAX_RETRIES")
    retry_base_delay_seconds: int = Field(default=30, alias="RETRY_BASE_DELAY_SECONDS")

    media_group_window_seconds: float = Field(default=3.0, alias="MEDIA_GROUP_WINDOW_SECONDS")
    http_timeout_seconds: float = Field(default=60.0, alias="HTTP_TIMEOUT_SECONDS")
    poll_timeout_seconds: int = Field(default=30, alias="POLL_TIMEOUT_SECONDS")

    # http  -> eitaayar / bot API over httpx
    # web   -> Selenium driving a logged-in web.eitaa.com session (Windows server)
    transport: Literal["http", "web"] = Field(default="http", alias="TRANSPORT")
    chrome_profile_dir: Path = Field(
        default=BASE_DIR / "data" / "chrome-profile", alias="CHROME_PROFILE_DIR"
    )
    browser: Literal["chrome", "edge"] = Field(default="chrome", alias="BROWSER")
    chrome_binary: str = Field(default="", alias="CHROME_BINARY")
    chromedriver_path: str = Field(default="", alias="CHROMEDRIVER_PATH")
    chrome_window_size: str = Field(default="1280,900", alias="CHROME_WINDOW_SIZE")
    web_headless: bool = Field(default=True, alias="WEB_HEADLESS")
    web_page_load_seconds: float = Field(default=8.0, alias="WEB_PAGE_LOAD_SECONDS")
    web_chat_open_seconds: float = Field(default=3.0, alias="WEB_CHAT_OPEN_SECONDS")
    web_send_settle_seconds: float = Field(default=1.5, alias="WEB_SEND_SETTLE_SECONDS")
    web_attach_settle_seconds: float = Field(default=3.0, alias="WEB_ATTACH_SETTLE_SECONDS")
    web_poll_interval_seconds: float = Field(default=5.0, alias="WEB_POLL_INTERVAL_SECONDS")
    web_history_page_size: int = Field(default=20, alias="WEB_HISTORY_PAGE_SIZE")

    daily_report_time: str = Field(default="23:45", alias="DAILY_REPORT_TIME")
    timezone: str = Field(default="Asia/Tehran", alias="TIMEZONE")

    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    log_file: Path = Field(default=BASE_DIR / "data" / "logs" / "bot.log", alias="LOG_FILE")
    log_max_bytes: int = Field(default=5 * 1024 * 1024, alias="LOG_MAX_BYTES")
    log_backup_count: int = Field(default=7, alias="LOG_BACKUP_COUNT")

    # Reserved for the future "repeat last N posts after X idle hours" feature.
    # The plumbing exists (see repeater.py) but stays disabled until switched on.
    repeat_enabled: bool = Field(default=False, alias="REPEAT_ENABLED")
    repeat_idle_hours: float = Field(default=1.0, alias="REPEAT_IDLE_HOURS")
    repeat_last_n_posts: int = Field(default=10, alias="REPEAT_LAST_N_POSTS")
    repeat_poll_seconds: float = Field(default=60.0, alias="REPEAT_POLL_SECONDS")

    active_start_hour: int = Field(default=5, alias="ACTIVE_START_HOUR")
    active_end_hour: int = Field(default=24, alias="ACTIVE_END_HOUR")
    repeat_active_start_hour: int = Field(default=5, alias="REPEAT_ACTIVE_START_HOUR")
    repeat_active_end_hour: int = Field(default=24, alias="REPEAT_ACTIVE_END_HOUR")

    # Every genuine new channel post also re-sends its N most recent
    # predecessors (default 9, so 10 messages total go out per new post).
    # Unrelated to the idle repeater above — this fires on every new post,
    # not after a quiet period.
    bundle_enabled: bool = Field(default=False, alias="BUNDLE_ENABLED")
    bundle_previous_count: int = Field(default=9, alias="BUNDLE_PREVIOUS_COUNT")

    panel_host: str = Field(default="127.0.0.1", alias="PANEL_HOST")
    panel_port: int = Field(default=8088, alias="PANEL_PORT")
    panel_username: str = Field(default="", alias="PANEL_USERNAME")
    panel_password: str = Field(default="", alias="PANEL_PASSWORD")


    @property
    def admin_ids(self) -> tuple[int, ...]:
        ids: list[int] = []
        for chunk in self.admin_ids_raw.replace(";", ",").split(","):
            chunk = chunk.strip()
            if not chunk:
                continue
            try:
                ids.append(int(chunk))
            except ValueError:
                continue
        return tuple(ids)


    @property
    def tz(self) -> ZoneInfo:
        try:
            return ZoneInfo(self.timezone)
        except ZoneInfoNotFoundError:
            logging.getLogger("eitaa.config").warning(
                "timezone %s unavailable (install the tzdata package); falling back to UTC",
                self.timezone,
            )
            return ZoneInfo("UTC")


    @property
    def report_hour_minute(self) -> tuple[int, int]:
        try:
            hour_str, minute_str = self.daily_report_time.split(":", 1)
            return int(hour_str), int(minute_str)
        except ValueError:
            return 23, 45


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]


def is_within_active_window(settings: Settings | None = None, when: dt.datetime | None = None) -> bool:
    settings = settings or get_settings()
    local = when or dt.datetime.now(settings.tz)
    if local.tzinfo is None:
        local = local.replace(tzinfo=settings.tz)
    else:
        local = local.astimezone(settings.tz)
    start, end = settings.active_start_hour, settings.active_end_hour
    if end >= 24:
        return local.hour >= start
    return start <= local.hour < end


def seconds_until_active_window(settings: Settings | None = None) -> float:
    settings = settings or get_settings()
    now = dt.datetime.now(settings.tz)
    if is_within_active_window(settings, now):
        return 0.0
    start = now.replace(hour=settings.active_start_hour, minute=0, second=0, microsecond=0)
    if now >= start:
        start += dt.timedelta(days=1)
    return max(0.0, (start - now).total_seconds())


def seconds_until_active_window_end(settings: Settings | None = None) -> float:
    settings = settings or get_settings()
    now = dt.datetime.now(settings.tz)
    if not is_within_active_window(settings, now):
        return 0.0
    end_hour = settings.active_end_hour
    if end_hour >= 24:
        end = now.replace(hour=0, minute=0, second=0, microsecond=0) + dt.timedelta(days=1)
    else:
        end = now.replace(hour=end_hour, minute=0, second=0, microsecond=0)
        if end <= now:
            end += dt.timedelta(days=1)
    return max(0.0, (end - now).total_seconds())


def setup_logging(settings: Settings | None = None) -> None:
    """Configure rotating file logging plus stdout logging once at startup."""
    ensure_utf8_console()
    settings = settings or get_settings()
    settings.log_file.parent.mkdir(parents=True, exist_ok=True)
    settings.files_dir.mkdir(parents=True, exist_ok=True)

    root = logging.getLogger()
    if root.handlers:
        return

    root.setLevel(settings.log_level.upper())
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)-16s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = logging.handlers.RotatingFileHandler(
        settings.log_file,
        maxBytes=settings.log_max_bytes,
        backupCount=settings.log_backup_count,
        encoding="utf-8",
    )
    file_handler.setFormatter(fmt)
    root.addHandler(file_handler)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(fmt)
    root.addHandler(stream_handler)

    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
