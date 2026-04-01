"""
services/telegram_bot.py
Telegram bot for the NSW Property Tracker.

Two modes:
  1. push_alert() — send a message to TELEGRAM_CHAT_ID (no polling needed)
  2. run_bot()    — start polling for commands (blocking, run in background process)

Commands:
  /old_listings            — listings >= 60 days
  /old_listings 90         — custom days
  /stats                   — summary stats
  /suburb Surry Hills      — filter by suburb
  /search 60 under 1000000 — 60+ days, under $1M
  /help                    — command list
"""
import json
import logging
import re
import urllib.parse
import urllib.request
from typing import Optional

import config
from services.aging import get_stale_listings, get_summary_stats, format_aged_report
from db.models import get_stats

logger = logging.getLogger(__name__)

API = f"https://api.telegram.org/bot{config.TELEGRAM_TOKEN}"


# ── Low-level API ──────────────────────────────────────────────────────────────

def _api_call(method: str, payload: dict) -> Optional[dict]:
    data = json.dumps(payload).encode("utf-8")
    req  = urllib.request.Request(
        f"{API}/{method}",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read())
    except Exception as e:
        logger.error("telegram: API call %s failed: %s", method, e)
        return None


def send_message(text: str, chat_id: str = None, parse_mode: str = "Markdown") -> bool:
    chat_id = chat_id or config.TELEGRAM_CHAT_ID
    if not chat_id or not config.TELEGRAM_TOKEN:
        logger.warning("telegram: not configured — skipping message")
        return False

    # Split messages longer than 4000 chars (Telegram limit = 4096)
    chunks = [text[i:i+4000] for i in range(0, len(text), 4000)]
    success = True
    for chunk in chunks:
        result = _api_call("sendMessage", {
            "chat_id":    chat_id,
            "text":       chunk,
            "parse_mode": parse_mode,
            "disable_web_page_preview": True,
        })
        if not result or not result.get("ok"):
            success = False
    return success


def push_alert(text: str) -> bool:
    """Send a one-way alert. Used by cron jobs."""
    return send_message(text)


# ── Command handlers ───────────────────────────────────────────────────────────

def _cmd_old_listings(args: list[str]) -> str:
    days = config.AGING_DAYS
    if args and args[0].isdigit():
        days = int(args[0])
    listings = get_stale_listings(days=days)
    return format_aged_report(listings, days=days)


def _cmd_stats(_: list[str]) -> str:
    s = get_summary_stats()
    return (
        f"📊 *Property Tracker Stats*\n\n"
        f"Active listings: {s['total_active']}\n"
        f"New today: {s['new_today']}\n"
        f"≥ 30 days: {s['aged_30_days']}\n"
        f"≥ 60 days: {s['aged_60_days']}\n"
        f"≥ 90 days: {s['aged_90_days']}\n"
    )


def _cmd_suburb(args: list[str]) -> str:
    if not args:
        return "Usage: /suburb <suburb name>\nExample: /suburb Surry Hills"
    suburb  = " ".join(args)
    listings = get_stale_listings(suburb=suburb)
    return format_aged_report(listings)


def _cmd_search(args: list[str]) -> str:
    """
    /search [days] [under PRICE] [over PRICE] [beds N] [suburb NAME]
    Example: /search 60 under 1000000 beds 3
    """
    text = " ".join(args).lower()

    # Parse days
    days_m = re.search(r"^(\d+)", text)
    days   = int(days_m.group(1)) if days_m else config.AGING_DAYS

    # Parse price filters
    under_m = re.search(r"under\s+(\d[\d,]*)", text)
    over_m  = re.search(r"over\s+(\d[\d,]*)", text)
    max_price = int(under_m.group(1).replace(",","")) if under_m else None
    min_price = int(over_m.group(1).replace(",",""))  if over_m  else None

    # Parse beds
    beds_m  = re.search(r"beds?\s+(\d+)", text)
    min_beds = int(beds_m.group(1)) if beds_m else None

    # Parse suburb
    suburb_m = re.search(r"in\s+([a-z\s]+?)(?:\s+\d|\s+under|\s+over|$)", text)
    suburb   = suburb_m.group(1).strip().title() if suburb_m else None

    listings = get_stale_listings(
        days=days, max_price=max_price, min_price=min_price,
        min_beds=min_beds, suburb=suburb
    )

    filters = []
    if suburb:    filters.append(f"suburb={suburb}")
    if max_price: filters.append(f"under ${max_price:,}")
    if min_price: filters.append(f"over ${min_price:,}")
    if min_beds:  filters.append(f"{min_beds}+ beds")

    header = f"🔍 Search: {days}+ days" + (f" | {', '.join(filters)}" if filters else "")
    return format_aged_report(listings, days=days).replace(
        f"🏠 *{len(listings)}", f"🏠 *{len(listings)} ({header})\n"
    )


def _cmd_help(_: list[str]) -> str:
    return (
        "🏠 *NSW Property Tracker*\n\n"
        "/old\\_listings — properties ≥ 60 days\n"
        "/old\\_listings 90 — properties ≥ 90 days\n"
        "/stats — summary dashboard\n"
        "/suburb Surry Hills — filter by suburb\n"
        "/search 60 under 1000000 — custom search\n"
        "/search 90 beds 3 in Paddington\n"
        "/help — this message\n"
    )


COMMANDS = {
    "/old_listings": _cmd_old_listings,
    "/oldlistings":  _cmd_old_listings,
    "/old":          _cmd_old_listings,
    "/stats":        _cmd_stats,
    "/suburb":       _cmd_suburb,
    "/search":       _cmd_search,
    "/help":         _cmd_help,
    "/start":        _cmd_help,
}


# ── Polling bot ────────────────────────────────────────────────────────────────

def _handle_update(update: dict):
    msg = update.get("message", {})
    text = msg.get("text", "").strip()
    chat_id = str(msg.get("chat", {}).get("id", ""))
    if not text or not chat_id:
        return

    parts   = text.split()
    command = parts[0].lower().split("@")[0]  # handle /cmd@botname
    args    = parts[1:]

    handler = COMMANDS.get(command)
    if handler:
        try:
            response = handler(args)
        except Exception as e:
            logger.error("telegram: handler error for %s: %s", command, e)
            response = f"❌ Error: {e}"
    else:
        response = f"Unknown command: {command}\n\n" + _cmd_help([])

    send_message(response, chat_id=chat_id)


def run_bot():
    """
    Long-polling bot loop. Run in a separate process or thread.
    Stops on KeyboardInterrupt.
    """
    if not config.TELEGRAM_TOKEN:
        logger.error("telegram: TELEGRAM_TOKEN not set — cannot run bot")
        return

    logger.info("telegram: bot starting (polling)...")
    offset = None

    while True:
        try:
            params = {"timeout": 30, "allowed_updates": ["message"]}
            if offset:
                params["offset"] = offset

            url = f"{API}/getUpdates?" + urllib.parse.urlencode(params)
            req = urllib.request.Request(url, method="GET")
            with urllib.request.urlopen(req, timeout=35) as resp:
                data = json.loads(resp.read())

            if data.get("ok"):
                for update in data.get("result", []):
                    offset = update["update_id"] + 1
                    _handle_update(update)

        except KeyboardInterrupt:
            logger.info("telegram: bot stopped")
            break
        except Exception as e:
            logger.error("telegram: polling error: %s", e)
            import time; time.sleep(5)


# ── Daily push ─────────────────────────────────────────────────────────────────

def send_daily_report():
    """Called by cron at DAILY_PUSH_HOUR."""
    stats    = get_summary_stats()
    aged     = get_stale_listings()
    report   = format_aged_report(aged)
    header   = (
        f"🌅 *Daily Property Report* — {__import__('datetime').date.today()}\n"
        f"Total active: {stats['total_active']} | New today: {stats['new_today']}\n\n"
    )
    push_alert(header + report)
