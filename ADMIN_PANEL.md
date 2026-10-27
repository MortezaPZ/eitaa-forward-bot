# Group admin panel

The panel adds, removes, enables, and disables groups, and lists them. Pause and resume stay on the command line (`manage.py`).

## Run

From the project directory:

```bat
.venv\Scripts\python.exe -m uvicorn web_panel:app --host 127.0.0.1 --port 8088
```

If `PANEL_PORT` in `.env` is different, pass that port.

For a permanent Windows service, use `EitaaPanel` in `WINDOWS_DEPLOY.md`.

## Open it

- On the server: `http://127.0.0.1:8088`
- From another machine on the private network: `http://<server-ip>:8088`
- On a locked-down network, forward port 8088 with an SSH tunnel and open `http://127.0.0.1:8088`

## Credentials

Read from `.env`:

- `PANEL_USERNAME`
- `PANEL_PASSWORD`

The browser shows a Basic Auth prompt.

## What the page does

- The top line shows the group count, how many are active, and whether sending is paused. Pause is display-only here.
- The add form accepts a group link, an `@username`, or a numeric id.
- Enable and disable keep the row and change the active flag.
- Delete asks the browser to confirm, then removes the group.
- Columns: group id, name or note, status (active, disabled, or kicked), recent error count, last successful send.

An Excel file and `manage.py` remain the backup path.

## Security

The panel is a simple password without HTTPS. Use it only on a private network, not on the open internet.
