# استقرار روی ویندوز (`TRANSPORT=web`)

روش تولید این پروژه: **Windows Server + Microsoft Edge + Selenium**. فایل‌های Docker و systemd فقط حالت قدیمی HTTP روی لینوکس هستند.

## پیش‌نیاز

- Python 3.11+ و محیط مجازی `.venv`
- Microsoft Edge و `msedgedriver.exe` هم‌نسخه (طبق `HANDOFF.md` بخش ۷)
- فایل `.env` با `TRANSPORT=web` و `BROWSER=edge`

## ۱) اولین لاگین (اجباری، روی دسکتاپ واقعی)

**مهم:** `python manage.py login` به پنجرهٔ قابل‌مشاهده نیاز دارد. این کار را **قبل از نصب سرویس** و در یک نشست دسکتاپ واقعی انجام دهید — نه از داخل سرویس ویندوز که بدون دسکتاپ اجرا می‌شود.

```bat
cd C:\path\to\project
.venv\Scripts\python.exe manage.py login
```

بعد از ورود موفق، در `.env` بگذارید:

```
WEB_HEADLESS=true
```

تا سرویس بدون پنجرهٔ قابل‌مشاهده اجرا شود.

## ۲) نصب NSSM

از سایت رسمی دانلود کنید: [https://nssm.cc](https://nssm.cc)

`nssm.exe` را در PATH بگذارید (مثلاً `C:\Windows\System32`).

مسیرهای زیر را با مسیر واقعی پروژه عوض کنید.

## ۳) سرویس ربات (`EitaaBot`)

```bat
nssm install EitaaBot "C:\path\to\.venv\Scripts\python.exe" "C:\path\to\main.py"
nssm set EitaaBot AppDirectory "C:\path\to\project"
nssm set EitaaBot AppExit Default Restart
nssm set EitaaBot AppRestartDelay 10000
nssm start EitaaBot
```

وضعیت:

```bat
nssm status EitaaBot
```

توقف:

```bat
nssm stop EitaaBot
```

لاگ فایل: `data\logs\bot.log`

اگر نشست مرورگر بعد از ۳ تلاش (۳۰، ۶۰، ۱۲۰ ثانیه) برنگردد، فرآیند با کد غیرصفر خارج می‌شود و NSSM دوباره آن را بالا می‌آورد.

## ۴) سرویس پنل مدیریت گروه‌ها (`EitaaPanel`)

پنل جدا از ربات اجرا می‌شود تا کرش یکی، دیگری را نخواباند.

```bat
nssm install EitaaPanel "C:\path\to\.venv\Scripts\python.exe" "-m" "uvicorn" "web_panel:app" "--host" "127.0.0.1" "--port" "8088"
nssm set EitaaPanel AppDirectory "C:\path\to\project"
nssm set EitaaPanel AppExit Default Restart
nssm set EitaaPanel AppRestartDelay 10000
nssm start EitaaPanel
```

اگر `PANEL_PORT` در `.env` چیز دیگری است، همان عدد را در `--port` بگذارید. `PANEL_HOST` را روی `127.0.0.1` نگه دارید مگر پشت شبکهٔ داخلی امن باشید.

وضعیت / توقف:

```bat
nssm status EitaaPanel
nssm stop EitaaPanel
```

راهنمای کاربری پنل: [راهنمای-پنل-مدیریت.md](راهنمای-پنل-مدیریت.md)

**امنیت:** این پنل Basic Auth ساده و بدون HTTPS است. روی اینترنت عمومی باز نکنید مگر پشت reverse proxy با HTTPS (خارج از این راهنما).
