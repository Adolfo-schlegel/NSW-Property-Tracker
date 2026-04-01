"""
config.py — Central configuration loaded from environment / .env file
"""
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

# ── Database ──────────────────────────────────────────────────────────────────
DB_PATH = os.getenv("DB_PATH", str(Path(__file__).parent / "db" / "properties.db"))

# ── Scraping ──────────────────────────────────────────────────────────────────
SCRAPE_HEADLESS    = os.getenv("SCRAPE_HEADLESS", "true").lower() == "true"
SCRAPE_TIMEOUT_MS  = int(os.getenv("SCRAPE_TIMEOUT_MS", "30000"))
SCRAPE_DELAY_MS    = int(os.getenv("SCRAPE_DELAY_MS", "2000"))   # between pages
MAX_PAGES          = int(os.getenv("MAX_PAGES", "50"))
RETRY_ATTEMPTS     = int(os.getenv("RETRY_ATTEMPTS", "3"))

# Suburbs to track (comma-separated). Empty = all Sydney.
SUBURBS = [s.strip() for s in os.getenv("SUBURBS", "").split(",") if s.strip()]

# Listing type: "sale" or "rent"
LISTING_TYPE = os.getenv("LISTING_TYPE", "sale")

# Minimum price (filter cheap listings)
MIN_PRICE = int(os.getenv("MIN_PRICE", "0"))
MAX_PRICE = int(os.getenv("MAX_PRICE", "0"))  # 0 = no limit

# ── Aging ─────────────────────────────────────────────────────────────────────
AGING_DAYS = int(os.getenv("AGING_DAYS", "60"))

# ── Google Sheets ─────────────────────────────────────────────────────────────
SHEETS_ENABLED         = os.getenv("SHEETS_ENABLED", "false").lower() == "true"
SHEETS_CREDENTIALS_FILE = os.getenv("SHEETS_CREDENTIALS_FILE", "google_credentials.json")
SHEETS_SPREADSHEET_ID  = os.getenv("SHEETS_SPREADSHEET_ID", "")
SHEETS_ALL_TAB         = os.getenv("SHEETS_ALL_TAB", "all_properties")
SHEETS_AGED_TAB        = os.getenv("SHEETS_AGED_TAB", "aged_60_days")

# ── Telegram ──────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TELEGRAM_ENABLED = bool(TELEGRAM_TOKEN and TELEGRAM_CHAT_ID)

# Daily push time (24h format, UTC)
DAILY_PUSH_HOUR = int(os.getenv("DAILY_PUSH_HOUR", "22"))  # 22 UTC = 9 AM AEDT
