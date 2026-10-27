# Eitaa Forward Bot

Forward every new post from an Eitaa channel to as many as 70 groups, after a delay, with a rate limit and a queue that survives a restart.

## Overview

The production path drives a logged-in Eitaa web session on Windows. An older HTTP bot API path still exists for Linux, but it cannot receive channel posts on the gateway that was tested. The queue, rate limit, retries, and reports are the same in both modes.

## Features

- Text, photo, video, file, audio, voice, and albums
- Albums stay one unit, in order, when the web session can forward the original message ids
- Default delay of 60 seconds so an edit in that window is what gets sent
- SQLite queue
- Interval, a per-minute cap, and jitter
- HTTP 429 backoff that honors `retry_after`
- Retries (default 3, with a growing delay)
- Admin commands, or `manage.py` when the in-app commands are unavailable
- Groups that kick the bot, or remove send permission, are disabled and the admin is told
- Daily report, rotating file log, and a full log in the database
- Duplicate protection on `message_id` and on the post/group pair
- Clean shutdown on SIGTERM

## Technology Stack

- Python 3.11+
- SQLAlchemy and SQLite
- httpx for the HTTP transport
- Selenium and Microsoft Edge for `TRANSPORT=web`
- FastAPI panel for the group list
- Docker and systemd only for the legacy HTTP transport

## Architecture

| Module | Role |
| --- | --- |
| `config.py` | Settings from `.env` and the rotating log |
| `db.py` | Groups, posts, media, queue, log, settings |
| `eitaa_client.py` | Async HTTP client for `botapi` and `eitaayar` |
| `web_transport.py` | Logged-in Chrome/Edge session. Same transport contract |
| `ingest.py` | Long polling, album assembly, file cache |
| `queue_worker.py` | Fan-out, rate limit, send, retry |
| `commands.py` | Admin commands |
| `reporter.py` | Daily report |
| `repeater.py` | Optional replay of the last N posts. Off unless enabled |
| `main.py` | Startup and shutdown |
| `manage.py` | CLI for the modes that have no in-app commands |
| `web_panel.py` | Group admin panel |

Eitaa does not offer a reliable `forwardMessage` for bots, so the HTTP path rebuilds the post from `file_id` or the cached file. Each media file is cached once under `FILES_DIR`.

## Installation

Production is Windows with `TRANSPORT=web`. The full service install is `WINDOWS_DEPLOY.md`. The group panel is `ADMIN_PANEL.md`.

```bash
pip install -r requirements.txt
```

Set `TRANSPORT=web` and `WEB_HEADLESS=false`, then:

```bash
python manage.py login
python manage.py probe
```

`login` opens a browser. The account owner signs in. The session stays in `CHROME_PROFILE_DIR`. `probe` reports which selectors were found.

- `strategy: js` means send goes through the page's message manager.
- `strategy: ui` means the composer has to be clicked. That path is slower.

If a selector is `MISS`, put the correct value in `selectors.json`. No code change is required.

```json
{
  "composer": "div.input-message-input",
  "file_input": "input[type='file']"
}
```

The legacy Linux HTTP install is not the production path:

```bash
sudo apt update && sudo apt install -y python3.11 python3.11-venv git
sudo useradd -m -d /opt/eitaa-bot -s /bin/bash eitaa
sudo mkdir -p /opt/eitaa-bot && sudo chown -R eitaa:eitaa /opt/eitaa-bot
sudo -u eitaa bash -lc 'cd /opt/eitaa-bot && python3.11 -m venv venv && ./venv/bin/pip install --upgrade pip'
sudo -u eitaa bash -lc 'cd /opt/eitaa-bot && ./venv/bin/pip install -r requirements.txt && cp .env.example .env'
sudo cp /opt/eitaa-bot/eitaa-bot.service /etc/systemd/system/eitaa-bot.service
sudo systemctl daemon-reload && sudo systemctl enable --now eitaa-bot
```

Docker for that same legacy path:

```bash
cp .env.example .env
docker compose up -d --build
docker compose logs -f
```

Database, cache, and logs stay in `./data`.

## Usage

In-app commands work only in a private chat, only for ids in `ADMIN_IDS`, and only when `EITAA_API_MODE=botapi`.

| Command | Effect |
| --- | --- |
| `/add <id or link>` | Add a group. Numeric id, `@username`, or an `eitaa.com` link |
| `/remove <id>` | Delete the group |
| `/list` | Groups, status, and the last error |
| `/disable <id>` | Disable without deleting |
| `/enable <id>` | Enable and clear the error counter |
| `/pause` | Stop sending. Ingest keeps queueing |
| `/resume` | Send again |
| `/delay <seconds>` | Delay between the channel post and the fan-out |
| `/rate <seconds>` | Gap between two sends |
| `/perminute <number>` | Cap per minute |
| `/status` | Queue, last send, recent errors |
| `/report` | Today's report |
| `/retry` | Requeue failed sends |
| `/help` | Help |

| Key | Default | Meaning |
| --- | --- | --- |
| `SEND_DELAY_SECONDS` | `60` | Wait before a post is sent |
| `SEND_INTERVAL_SECONDS` | `1.0` | Base gap between sends |
| `SEND_JITTER_RATIO` | `0.35` | Randomize the gap |
| `MAX_SENDS_PER_MINUTE` | `45` | Per-minute cap |
| `MAX_RETRIES` | `3` | Retries per send |
| `RETRY_BASE_DELAY_SECONDS` | `30` | Exponential backoff base |
| `MEDIA_GROUP_WINDOW_SECONDS` | `3.0` | How long to collect an album |
| `DAILY_REPORT_TIME` | `23:45` | Daily report clock time |

CLI, including the gateway mode that has no in-app commands:

```bash
python manage.py check
python manage.py add https://eitaa.com/joinchat/XXXX --title "Group one"
python manage.py import groups.txt
python manage.py list
python manage.py status
python manage.py report
python manage.py set delay_seconds 60
python manage.py retry
python manage.py publish --text "Post text"
python manage.py publish --file photo.jpg --type photo --text "Caption"
```

`publish` enters the same queue as channel ingest, so delay, rate limit, retry, and reports apply.

Optional idle replay:

```
REPEAT_ENABLED=true
REPEAT_IDLE_HOURS=6
REPEAT_LAST_N_POSTS=3
```

The repeater checks every 5 minutes. If the channel has been idle, it copies recent posts with `dispatched=false` and the normal queue sends them.

HTTP methods that were checked against the Eitaa gateway: `getMe` works. `sendMessage` and `sendFile` match the documented send API. `getUpdates`, `setWebhook`, and `getChats` returned `method not found`. Automatic channel ingest therefore uses the web session (`messages.getHistory`), not `getUpdates`.

## Testing

The offline suite is `pytest`. A full Selenium run needs a logged-in desktop session and is not part of the default test command.

## Limitations

- The admin panel is Basic Auth without HTTPS. Use it on a private network. See `ADMIN_PANEL.md`.
- `composer` and `file_input` are absent from the DOM until a chat is open. `probe` can report `MISS` immediately after load even when the selector is right.
- Web forward needs a numeric peer id. A group stored only as an invite link has to be resolved first.
- A web UI change can break selectors. Pin replacements in `selectors.json`.
- A real send takes about 1 to 5 seconds. Keep `SEND_INTERVAL_SECONDS` conservative.
- Posts created with `manage.py publish` have no source message ids, so their album parts are sent separately.
- Chromedriver downloads are geo-blocked from Iran. The working browser is Microsoft Edge, and `msedgedriver` must match the installed Edge version.
- `tzdata` is required on Windows.

## License

See the repository license file if one is present.
