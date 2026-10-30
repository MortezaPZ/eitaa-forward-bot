# Windows deployment (`TRANSPORT=web`)

Production for this project is Windows Server, Microsoft Edge, and Selenium. The Docker and systemd files are the older HTTP transport on Linux.

## Requirements

- Python 3.11+ and a `.venv`
- Microsoft Edge and an msedgedriver.exe matching the installed Edge version; set CHROMEDRIVER_PATH in the local .env file.
- `.env` with `TRANSPORT=web` and `BROWSER=edge`

## 1. First login

`python manage.py login` needs a visible window. Do this on a real desktop session before you install the Windows service. A service session has no desktop.

```bat
cd C:\path\to\project
.venv\Scripts\python.exe manage.py login
```

After a successful login, set:

```
WEB_HEADLESS=true
```

The service can then run without a visible window.

## 2. Install NSSM

Download it from [https://nssm.cc](https://nssm.cc) and put `nssm.exe` on `PATH`, for example `C:\Windows\System32`.

Replace the paths below with the real project path.

## 3. Bot service (`EitaaBot`)

```bat
nssm install EitaaBot "C:\path\to\.venv\Scripts\python.exe" "C:\path\to\main.py"
nssm set EitaaBot AppDirectory "C:\path\to\project"
nssm set EitaaBot AppStdout "C:\path\to\project\data\service-stdout.log"
nssm set EitaaBot AppStderr "C:\path\to\project\data\service-stderr.log"
nssm set EitaaBot AppRotateFiles 1
nssm set EitaaBot Start SERVICE_AUTO_START
nssm start EitaaBot
```

## 4. Group panel service (`EitaaPanel`)

```bat
nssm install EitaaPanel "C:\path\to\.venv\Scripts\python.exe" "-m" "uvicorn" "web_panel:app" "--host" "127.0.0.1" "--port" "8088"
nssm set EitaaPanel AppDirectory "C:\path\to\project"
nssm set EitaaPanel Start SERVICE_AUTO_START
nssm start EitaaPanel
```

If `PANEL_PORT` in `.env` is not 8088, use that port in the `uvicorn` argument.

Panel behavior is described in `ADMIN_PANEL.md`.
