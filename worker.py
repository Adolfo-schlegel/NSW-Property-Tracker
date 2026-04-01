#!/usr/bin/env python3
"""
NSW Property Tracker — Daily Worker
====================================
Runs on the server. Scrapes Domain API and ingests results.
REA requires running rea_push_client.py from a residential IP.

Logs to: /var/log/property-tracker/worker.log
Notify:  sends Telegram message with daily summary
"""

import sys, os, json, logging, requests
from datetime import date, datetime
from pathlib import Path

# ── Paths ────────────────────────────────────────────────────────────────────
BASE_DIR   = Path(__file__).parent
LOG_DIR    = Path("/var/log/property-tracker")
LOG_FILE   = LOG_DIR / "worker.log"
DB_PATH    = str(BASE_DIR / "properties.db")
STATE_FILE = BASE_DIR / "worker_state.json"

# ── Telegram notify (optional) ───────────────────────────────────────────────
# Set these in /etc/property-tracker.env or leave blank to skip
TELEGRAM_TOKEN  = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT   = os.environ.get("TELEGRAM_CHAT",  "8056518846")
INGEST_URL      = os.environ.get("INGEST_URL", "http://localhost:8765/api/ingest")

# ── Setup logging ─────────────────────────────────────────────────────────────
LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger("worker")


# ── Domain scraper (inline, no dep on scraper/ dir) ──────────────────────────
API_KEY   = os.environ.get("DOMAIN_API_KEY", "")
API_BASE  = "https://api.domain.com.au/v1"
POSTCODES = ["2110", "2111", "2112", "2113", "2114", "2115"]

def run_domain_scraper() -> list[dict]:
    """Scrape Domain API for all target postcodes."""
    sys.path.insert(0, str(BASE_DIR / "scraper"))
    try:
        from domain_scraper import scrape
        return scrape()
    except ImportError:
        log.warning("domain_scraper module not found, using inline fallback")
        return _domain_inline()

def _domain_inline() -> list[dict]:
    """Minimal inline Domain scraper if scraper/ dir isn't in path."""
    from datetime import date as _date
    results = []
    seen    = set()
    headers = {"X-Api-Key": API_KEY, "Content-Type": "application/json"}
    today   = _date.today().isoformat()

    for pc in POSTCODES:
        try:
            resp = requests.post(
                f"{API_BASE}/listings/residential/_search",
                headers=headers,
                json={
                    "listingType": "Sale",
                    "locations": [{"state": "NSW", "postCode": pc}],
                    "pageSize": 200,
                    "sort": {"sortKey": "dateUpdated", "direction": "Descending"},
                },
                timeout=30,
            )
        except Exception as e:
            log.error(f"Domain request failed for {pc}: {e}")
            continue

        if resp.status_code == 403:
            log.error("Domain API 403 — add 'Listings Management → Sandbox' at developer.domain.com.au")
            return []
        if resp.status_code != 200:
            log.error(f"Domain HTTP {resp.status_code} for {pc}")
            continue

        data = resp.json()
        if not isinstance(data, list):
            data = data.get("listings", [])

        for raw in data:
            listing = raw.get("listing", raw)
            addr    = listing.get("addressParts") or listing.get("address") or {}
            if isinstance(addr, str):
                addr = {}
            sid = f"domain_{listing.get('id','')}"
            if not sid or sid in seen:
                continue
            seen.add(sid)

            street_no   = addr.get("streetNumber","")
            street_name = addr.get("street","")
            street_type = addr.get("streetType","")
            suburb      = addr.get("suburb","")
            postcode    = addr.get("postcode", pc)
            address     = f"{street_no} {street_name} {street_type}, {suburb} NSW {postcode}".strip(", ")

            pd       = listing.get("priceDetails") or {}
            price_s  = pd.get("displayPrice","")
            price_v  = pd.get("price",0) or 0

            dl = listing.get("dateListed","")
            try:
                from datetime import datetime as dt
                dl = dt.fromisoformat(dl.replace("Z","")).strftime("%Y-%m-%d") if dl else today
            except Exception:
                dl = dl[:10] if dl else today

            from datetime import date as _d
            dom = (_d.today() - _d.fromisoformat(dl)).days

            feat   = listing.get("features") or {}
            pts    = listing.get("propertyTypes",[])
            ptype  = pts[0] if pts else listing.get("propertyType","")
            lid    = str(listing.get("id",""))
            url    = f"https://www.domain.com.au/{lid}" if lid else ""

            results.append({
                "source_id": sid, "source": "domain",
                "address": address, "street_no": street_no,
                "street_name": f"{street_name} {street_type}".strip(),
                "suburb": suburb, "state": "NSW", "postcode": postcode,
                "listing_type": "Sale", "property_type": ptype,
                "price": price_s, "price_value": price_v,
                "bedrooms":   feat.get("numBedrooms",0) or 0,
                "bathrooms":  feat.get("numBathrooms",0) or 0,
                "carspaces":  feat.get("numCarSpaces",0) or 0,
                "land_size":  listing.get("landArea",0) or 0,
                "first_seen": dl, "last_seen": today,
                "days_on_market": dom, "url": url, "raw": "",
            })

        log.info(f"  Domain {pc}: {len(data)} listings")

    return results


# ── Ingest into local DB ──────────────────────────────────────────────────────
def ingest_local(listings: list[dict]) -> dict:
    sys.path.insert(0, str(BASE_DIR / "scraper"))
    from dedup import ingest
    return ingest(listings, DB_PATH)


# ── Fix days_on_market in DB ──────────────────────────────────────────────────
def refresh_days_on_market():
    import sqlite3
    today = date.today()
    conn  = sqlite3.connect(DB_PATH)
    rows  = conn.execute("SELECT id, first_seen FROM properties WHERE first_seen IS NOT NULL").fetchall()
    for rid, fs in rows:
        try:
            dom = (today - date.fromisoformat(fs)).days
            conn.execute("UPDATE properties SET days_on_market=?, last_seen=? WHERE id=?",
                         (dom, today.isoformat(), rid))
        except Exception:
            pass
    conn.commit()
    conn.close()


# ── Telegram notify ───────────────────────────────────────────────────────────
def notify_telegram(message: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT, "text": message, "parse_mode": "Markdown"},
            timeout=10,
        )
    except Exception as e:
        log.warning(f"Telegram notify failed: {e}")


# ── Load/save state ───────────────────────────────────────────────────────────
def load_state() -> dict:
    try:
        return json.loads(STATE_FILE.read_text())
    except Exception:
        return {}

def save_state(state: dict):
    STATE_FILE.write_text(json.dumps(state, indent=2))


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    start  = datetime.now()
    today  = date.today().isoformat()
    state  = load_state()
    errors = []

    log.info("=" * 60)
    log.info(f"NSW Property Tracker — Daily Worker — {today}")
    log.info("=" * 60)

    # ── Domain scrape ─────────────────────────────────────────────
    log.info("Source 1: Domain.com.au API")
    domain_listings = []
    domain_stats    = {}
    try:
        domain_listings = run_domain_scraper()
        log.info(f"  Scraped: {len(domain_listings)} listings")

        if domain_listings:
            domain_stats = ingest_local(domain_listings)
            log.info(f"  Ingest: {domain_stats}")
        else:
            log.warning("  No listings returned from Domain (API package may not be activated)")
    except Exception as e:
        log.error(f"  Domain scraper error: {e}", exc_info=True)
        errors.append(f"Domain: {e}")

    # ── Refresh days on market for all entries ────────────────────
    log.info("Refreshing days_on_market for all records...")
    try:
        refresh_days_on_market()
        log.info("  Done")
    except Exception as e:
        log.error(f"  Refresh error: {e}")

    # ── Stats ─────────────────────────────────────────────────────
    import sqlite3
    try:
        conn  = sqlite3.connect(DB_PATH)
        total = conn.execute("SELECT COUNT(*) FROM properties WHERE is_duplicate=0").fetchone()[0]
        new_t = conn.execute("SELECT COUNT(*) FROM properties WHERE is_duplicate=0 AND first_seen=?", (today,)).fetchone()[0]
        a30   = conn.execute("SELECT COUNT(*) FROM properties WHERE is_duplicate=0 AND days_on_market>=30").fetchone()[0]
        a60   = conn.execute("SELECT COUNT(*) FROM properties WHERE is_duplicate=0 AND days_on_market>=60").fetchone()[0]
        dedup = conn.execute("SELECT COUNT(*) FROM properties WHERE is_duplicate=1").fetchone()[0]
        conn.close()
        db_stats = {"total": total, "today": new_t, "aged_30": a30, "aged_60": a60, "deduped": dedup}
    except Exception as e:
        log.error(f"Stats error: {e}")
        db_stats = {}

    elapsed = (datetime.now() - start).total_seconds()

    log.info("-" * 60)
    log.info(f"Completed in {elapsed:.1f}s")
    log.info(f"DB stats: {db_stats}")
    if errors:
        log.error(f"Errors: {errors}")
    log.info("=" * 60)

    # ── Save state ────────────────────────────────────────────────
    save_state({
        "last_run":     today,
        "last_elapsed": elapsed,
        "last_stats":   db_stats,
        "last_errors":  errors,
    })

    # ── Telegram summary ──────────────────────────────────────────
    if domain_listings or not errors:
        status = "✅" if not errors else "⚠️"
        msg = (
            f"{status} *NSW Property Tracker* — {today}\n\n"
            f"🏘 Total activos: *{db_stats.get('total',0)}*\n"
            f"✨ Nuevos hoy: *{db_stats.get('today',0)}*\n"
            f"⏳ 30+ días: *{db_stats.get('aged_30',0)}*\n"
            f"🔴 60+ días: *{db_stats.get('aged_60',0)}*\n"
            f"🔀 Deduplicados: *{db_stats.get('deduped',0)}*\n\n"
            f"🌐 https://tmntech.ddns.net/tracker/"
        )
        if errors:
            msg += f"\n\n⚠️ Errores: {'; '.join(errors)}"
        notify_telegram(msg)

    return 0 if not errors else 1


if __name__ == "__main__":
    sys.exit(main())
