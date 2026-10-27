import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.environ["EITAA_BOT_TOKEN"] = "test-token"
os.environ["SOURCE_CHANNEL_ID"] = "-100111"
os.environ["ADMIN_IDS"] = "42"
os.environ["DATABASE_URL"] = "sqlite+aiosqlite:///" + (ROOT / "data" / "pytest.db").as_posix()
os.environ["TRANSPORT"] = "http"
os.environ["EITAA_API_MODE"] = "botapi"
os.environ["FILES_DIR"] = str(ROOT / "data" / "pytest-files")
