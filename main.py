"""
main.py — Orchestrator
Usage:
  python main.py scrape          # run scraper (domain + realestate)
  python main.py scrape domain   # run only domain
  python main.py report          # push aged report to Telegram
  python main.py sheets          # sync Google Sheets
  python main.py bot             # start Telegram polling bot
  python main.py stats           # print summary to stdout
  python main.py all             # scrape + report + sheets
"""
import logging
import sys
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("tracker.log"),
    ],
)
logger = logging.getLogger("main")


def run_scraper(sources: list[str] = None):
    """Run scraper(s) and update the database."""
    from playwright.sync_api import sync_playwright
    from db.models import init_db, upsert_property, mark_inactive, log_run, finish_run
    from scraper.domain import DomainScraper
    from scraper.realestate import RealEstateScraper
    import config

    init_db()
    sources = sources or ["domain", "realestate"]

    for source in sources:
        started  = datetime.utcnow()
        run_id   = log_run(source, started)
        counts   = {"found": 0, "new": 0, "updated": 0, "removed": 0}
        error    = None

        logger.info("=== Starting scrape: %s ===", source)
        try:
            with sync_playwright() as pw:
                if source == "domain":
                    scraper = DomainScraper(pw, headless=config.SCRAPE_HEADLESS)
                elif source == "realestate":
                    scraper = RealEstateScraper(pw, headless=config.SCRAPE_HEADLESS)
                else:
                    logger.error("Unknown source: %s", source)
                    continue

                scraper.start()
                try:
                    listings = scraper.scrape(
                        listing_type=config.LISTING_TYPE,
                        max_pages=config.MAX_PAGES,
                    )
                finally:
                    scraper.stop()

            counts["found"] = len(listings)
            active_ids = set()

            for listing in listings:
                # Apply price filter
                pv = listing.get("price_value")
                if config.MAX_PRICE and pv and pv > config.MAX_PRICE:
                    continue
                if config.MIN_PRICE and pv and pv < config.MIN_PRICE:
                    continue

                result = upsert_property(listing)
                active_ids.add(listing["id"])

                if result == "new":           counts["new"] += 1
                elif result == "price_changed": counts["updated"] += 1
                else:                         counts["updated"] += 1

            # Mark inactive
            mark_inactive(active_ids, source)
            counts["removed"] = counts["found"] - len(active_ids)  # approximate

            logger.info(
                "[%s] done — found=%d new=%d updated=%d",
                source, counts["found"], counts["new"], counts["updated"]
            )

        except Exception as e:
            logger.exception("[%s] scrape failed: %s", source, e)
            error = str(e)

        finish_run(run_id, counts, error)


def run_report():
    """Push daily aged report to Telegram."""
    from services.telegram_bot import send_daily_report
    send_daily_report()
    logger.info("Report sent to Telegram")


def run_sheets():
    """Sync all active + aged properties to Google Sheets."""
    from services.sheets import sync_all_properties, sync_aged_properties
    from services.aging import get_stale_listings
    from db.models import get_all_active
    import config

    all_props  = get_all_active(listing_type=config.LISTING_TYPE)
    aged_props = get_stale_listings()

    sync_all_properties(all_props)
    sync_aged_properties(aged_props)
    logger.info("Sheets synced — %d total, %d aged", len(all_props), len(aged_props))


def run_bot():
    """Start Telegram polling bot (blocking)."""
    from services.telegram_bot import run_bot as _run_bot
    logger.info("Starting Telegram bot...")
    _run_bot()


def print_stats():
    """Print summary stats to stdout."""
    from db.models import init_db
    from services.aging import get_summary_stats, get_stale_listings
    import config

    init_db()
    s = get_summary_stats()
    aged = get_stale_listings()

    print(f"\n{'='*50}")
    print(f"NSW Property Tracker — Stats")
    print(f"{'='*50}")
    print(f"Total active listings : {s['total_active']}")
    print(f"New today             : {s['new_today']}")
    print(f"≥ 30 days on market   : {s['aged_30_days']}")
    print(f"≥ 60 days on market   : {s['aged_60_days']}")
    print(f"≥ 90 days on market   : {s['aged_90_days']}")
    print(f"\nTop 5 stalest listings:")
    for i, l in enumerate(aged[:5], 1):
        print(f"  {i}. {l.get('address','?')} — {l.get('price','?')} — {l.get('days_on_market','?')} days")
    print(f"{'='*50}\n")


if __name__ == "__main__":
    cmd     = sys.argv[1] if len(sys.argv) > 1 else "help"
    subcmds = sys.argv[2:]

    if cmd == "scrape":
        run_scraper(sources=subcmds if subcmds else None)
    elif cmd == "report":
        run_report()
    elif cmd == "sheets":
        run_sheets()
    elif cmd == "bot":
        run_bot()
    elif cmd == "stats":
        print_stats()
    elif cmd == "all":
        run_scraper()
        run_report()
        run_sheets()
    else:
        print(__doc__)
