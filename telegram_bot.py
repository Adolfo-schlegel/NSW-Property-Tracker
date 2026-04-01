#!/usr/bin/env python3
"""
NSW Property Tracker — Telegram Bot
====================================
100% rule-based, no AI.
Responds to postcodes and simple commands.

Commands:
  2110..2115  → listings for that postcode
  /all        → all active listings
  /new        → listed this week
  /stale      → 60+ days on market
  /cheap      → sorted by price ASC
  /help       → show commands
"""

import requests, sqlite3, json, time, logging, sys, os, signal
from datetime import date, datetime, timedelta
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────
TOKEN    = os.environ.get("TELEGRAM_TOKEN", "")
DB_PATH  = str(Path(__file__).parent / "properties.db")
API_BASE = f"https://api.telegram.org/bot{TOKEN}"
MAX_RESULTS = 10   # listings per message
POSTCODES   = {"2110","2111","2112","2113","2114","2115"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("/var/log/property-tracker/bot.log"),
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger("bot")

# ── DB ────────────────────────────────────────────────────────────────────────
def query(where="1=1", params=(), order="first_seen DESC", limit=MAX_RESULTS):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        f"SELECT * FROM properties WHERE is_duplicate=0 AND {where} ORDER BY {order} LIMIT {limit}",
        params
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def count(where="1=1", params=()):
    conn = sqlite3.connect(DB_PATH)
    n = conn.execute(f"SELECT COUNT(*) FROM properties WHERE is_duplicate=0 AND {where}", params).fetchone()[0]
    conn.close()
    return n

def stats():
    today = date.today().isoformat()
    conn  = sqlite3.connect(DB_PATH)
    c     = conn.cursor()
    r = {
        "total":   c.execute("SELECT COUNT(*) FROM properties WHERE is_duplicate=0").fetchone()[0],
        "today":   c.execute("SELECT COUNT(*) FROM properties WHERE is_duplicate=0 AND first_seen=?", (today,)).fetchone()[0],
        "aged_30": c.execute("SELECT COUNT(*) FROM properties WHERE is_duplicate=0 AND days_on_market>=30").fetchone()[0],
        "aged_60": c.execute("SELECT COUNT(*) FROM properties WHERE is_duplicate=0 AND days_on_market>=60").fetchone()[0],
    }
    conn.close()
    return r

# ── Format a listing ──────────────────────────────────────────────────────────
def fmt_listing(p, idx=None):
    prefix = f"{idx}. " if idx else "• "
    addr   = p.get("address","?").split(",")[0].strip()
    suburb = p.get("suburb","")
    price  = p.get("price") or "Price on request"
    beds   = p.get("bedrooms") or "?"
    baths  = p.get("bathrooms") or "?"
    dom    = p.get("days_on_market", 0)
    fs     = p.get("first_seen","")
    url    = p.get("url") or p.get("canonical_url") or ""

    age_flag = ""
    if dom >= 90:   age_flag = " 🔴 90+ days"
    elif dom >= 60: age_flag = " 🟡 60+ days"
    elif dom == 0:  age_flag = " 🟢 New"

    line = (
        f"{prefix}{addr}, {suburb}\n"
        f"   💰 {price}  🛏 {beds}  🚿 {baths}  📅 {fs}{age_flag}"
    )
    if url:
        line += f"\n   🔗 {url}"
    return line

# ── Handle a message → return reply text ─────────────────────────────────────
def handle(text: str, user_name: str) -> str:
    text = text.strip()
    cmd  = text.lower().lstrip("/")

    # ── Postcode ──────────────────────────────────────────────────
    if text in POSTCODES:
        rows  = query("postcode=?", (text,), "first_seen DESC", MAX_RESULTS)
        total = count("postcode=?", (text,))
        if not rows:
            return f"No listings found for postcode {text}."
        lines = [f"🏘 *Postcode {text}* — {total} listing{'s' if total!=1 else ''} (showing {len(rows)})\n"]
        for i, p in enumerate(rows, 1):
            lines.append(fmt_listing(p, i))
        return "\n".join(lines)

    # ── /all ──────────────────────────────────────────────────────
    if cmd in ("all", "listings"):
        s    = stats()
        rows = query(limit=MAX_RESULTS)
        lines = [
            f"🏘 *All Listings* — {s['total']} active\n"
            f"✨ New today: {s['today']}  |  ⏳ 30+ days: {s['aged_30']}  |  🔴 60+ days: {s['aged_60']}\n"
        ]
        for i, p in enumerate(rows, 1):
            lines.append(fmt_listing(p, i))
        if s['total'] > MAX_RESULTS:
            lines.append(f"\n_...and {s['total']-MAX_RESULTS} more. Visit tmntech.ddns.net/tracker/_")
        return "\n".join(lines)

    # ── /new ──────────────────────────────────────────────────────
    if cmd in ("new", "today", "latest"):
        week_ago = (date.today() - timedelta(days=7)).isoformat()
        rows  = query("first_seen >= ?", (week_ago,), "first_seen DESC", MAX_RESULTS)
        total = count("first_seen >= ?", (week_ago,))
        if not rows:
            return "No new listings in the past 7 days."
        lines = [f"✨ *New this week* — {total} listing{'s' if total!=1 else ''}\n"]
        for i, p in enumerate(rows, 1):
            lines.append(fmt_listing(p, i))
        return "\n".join(lines)

    # ── /stale ────────────────────────────────────────────────────
    if cmd in ("stale", "old", "slow"):
        rows  = query("days_on_market >= 60", order="days_on_market DESC", limit=MAX_RESULTS)
        total = count("days_on_market >= 60")
        if not rows:
            return "No listings over 60 days on market."
        lines = [f"🔴 *Stale listings (60+ days)* — {total} total\n"]
        for i, p in enumerate(rows, 1):
            lines.append(fmt_listing(p, i))
        return "\n".join(lines)

    # ── /cheap ────────────────────────────────────────────────────
    if cmd in ("cheap", "cheapest", "lowest"):
        rows  = query("price_value > 0", order="price_value ASC", limit=MAX_RESULTS)
        total = count("price_value > 0")
        if not rows:
            return "No listings with price data."
        lines = [f"💰 *Cheapest listings* — showing {len(rows)} of {total}\n"]
        for i, p in enumerate(rows, 1):
            lines.append(fmt_listing(p, i))
        return "\n".join(lines)

    # ── /expensive ────────────────────────────────────────────────
    if cmd in ("expensive", "top", "highest"):
        rows = query("price_value > 0", order="price_value DESC", limit=MAX_RESULTS)
        lines = [f"🏆 *Most expensive*\n"]
        for i, p in enumerate(rows, 1):
            lines.append(fmt_listing(p, i))
        return "\n".join(lines)

    # ── /stats ────────────────────────────────────────────────────
    if cmd in ("stats", "summary", "status"):
        s = stats()
        return (
            f"📊 *Property Tracker Stats*\n"
            f"Date: {date.today()}\n\n"
            f"🏘 Total active: *{s['total']}*\n"
            f"✨ New today: *{s['today']}*\n"
            f"⏳ 30+ days: *{s['aged_30']}*\n"
            f"🔴 60+ days: *{s['aged_60']}*\n\n"
            f"🌐 tmntech.ddns.net/tracker/"
        )

    # ── /ryde area shortcuts ──────────────────────────────────────
    suburb_shortcuts = {
        "ryde":        ("suburb LIKE ?", ("%Ryde%",)),
        "hunters":     ("suburb LIKE ?", ("%Hunters Hill%",)),
        "gladesville": ("suburb LIKE ?", ("%Gladesville%",)),
        "eastwood":    ("suburb LIKE ?", ("%Eastwood%",)),
        "meadowbank":  ("suburb LIKE ?", ("%Meadowbank%",)),
        "ermington":   ("suburb LIKE ?", ("%Ermington%",)),
        "putney":      ("suburb LIKE ?", ("%Putney%",)),
        "northryde":   ("suburb LIKE ?", ("%North Ryde%",)),
        "westryde":    ("suburb LIKE ?", ("%West Ryde%",)),
    }
    clean = cmd.replace(" ","").replace("-","")
    if clean in suburb_shortcuts:
        w, p  = suburb_shortcuts[clean]
        rows  = query(w, p, "first_seen DESC", MAX_RESULTS)
        total = count(w, p)
        suburb_display = clean.title()
        if not rows:
            return f"No listings found in {suburb_display}."
        lines = [f"📍 *{suburb_display}* — {total} listing{'s' if total!=1 else ''}\n"]
        for i, row in enumerate(rows, 1):
            lines.append(fmt_listing(row, i))
        return "\n".join(lines)

    # ── /help / /start ────────────────────────────────────────────
    if cmd in ("help", "start", ""):
        return (
            "🏠 *NSW Property Tracker Bot*\n\n"
            "*Send a postcode:*\n"
            "  2110  2111  2112  2113  2114  2115\n\n"
            "*Commands:*\n"
            "  /all        — all active listings\n"
            "  /new        — listed this week\n"
            "  /stale      — 60+ days on market\n"
            "  /cheap      — lowest price first\n"
            "  /expensive  — highest price first\n"
            "  /stats      — summary counts\n\n"
            "*Suburbs:*\n"
            "  /ryde  /hunters  /gladesville\n"
            "  /eastwood  /meadowbank  /putney\n"
            "  /northryde  /westryde  /ermington\n\n"
            "🌐 tmntech.ddns.net/tracker/"
        )

    # ── Basic keyword fallback (no AI, pure pattern matching) ────────────────
    t = text.lower()

    # Postcode anywhere in message
    for pc in POSTCODES:
        if pc in t:
            rows  = query("postcode=?", (pc,), "first_seen DESC", MAX_RESULTS)
            total = count("postcode=?", (pc,))
            if rows:
                lines = [f"🏘 *Postcode {pc}* — {total} listing{'s' if total!=1 else ''}\n"]
                for i, p in enumerate(rows, 1):
                    lines.append(fmt_listing(p, i))
                return "\n".join(lines)

    # Days on market keywords
    if any(x in t for x in ["60 day", "two month", "2 month", "stale", "slow"]):
        return handle("/stale", user_name)
    if any(x in t for x in ["30 day", "one month", "1 month", "month"]):
        rows  = query("days_on_market >= 30", order="days_on_market DESC", limit=MAX_RESULTS)
        total = count("days_on_market >= 30")
        lines = [f"⏳ *30+ days on market* — {total} total\n"]
        for i, p in enumerate(rows, 1):
            lines.append(fmt_listing(p, i))
        return "\n".join(lines)

    # Price keywords
    if any(x in t for x in ["cheap", "lowest", "affordable", "budget", "low price"]):
        return handle("/cheap", user_name)
    if any(x in t for x in ["expensive", "highest", "luxury", "top price"]):
        return handle("/expensive", user_name)

    # New/recent keywords
    if any(x in t for x in ["new", "latest", "recent", "today", "this week"]):
        return handle("/new", user_name)

    # Stats keywords
    if any(x in t for x in ["stats", "summary", "how many", "count", "total"]):
        return handle("/stats", user_name)

    # Suburb keywords
    for suburb_key in ["ryde", "hunters", "gladesville", "eastwood", "meadowbank", "ermington", "putney"]:
        if suburb_key in t:
            return handle(f"/{suburb_key}", user_name)

    return "Send /help to see all available commands.\n\nExamples:\n  2112\n  /new\n  /cheap\n  /stale"

# ── Telegram API helpers ──────────────────────────────────────────────────────
def send(chat_id: int, text: str):
    try:
        r = requests.post(f"{API_BASE}/sendMessage", json={
            "chat_id":    chat_id,
            "text":       text,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True,
        }, timeout=15)
        if not r.ok:
            log.warning(f"sendMessage {r.status_code}: {r.text[:200]}")
    except Exception as e:
        log.error(f"sendMessage error: {e}")

def get_updates(offset: int) -> list:
    try:
        r = requests.get(f"{API_BASE}/getUpdates", params={
            "offset":  offset,
            "timeout": 30,
        }, timeout=40)
        return r.json().get("result", []) if r.ok else []
    except Exception as e:
        log.error(f"getUpdates error: {e}")
        return []

# ── Main loop ─────────────────────────────────────────────────────────────────
def main():
    log.info("NSW Property Tracker Bot — starting")

    # Verify token
    me = requests.get(f"{API_BASE}/getMe", timeout=10).json()
    if not me.get("ok"):
        log.error(f"Invalid token: {me}")
        sys.exit(1)
    log.info(f"Connected as @{me['result']['username']}")

    offset = 0
    while True:
        updates = get_updates(offset)
        for upd in updates:
            offset = upd["update_id"] + 1
            msg    = upd.get("message") or upd.get("edited_message")
            if not msg:
                continue
            text    = msg.get("text","").strip()
            chat_id = msg["chat"]["id"]
            user    = msg.get("from",{}).get("first_name","?")
            if not text:
                continue
            log.info(f"[{user}] {text!r}")
            reply = handle(text, user)
            send(chat_id, reply)

        if not updates:
            time.sleep(1)

if __name__ == "__main__":
    # Make log dir
    Path("/var/log/property-tracker").mkdir(parents=True, exist_ok=True)

    # Handle graceful shutdown
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    try:
        main()
    except KeyboardInterrupt:
        log.info("Bot stopped.")
