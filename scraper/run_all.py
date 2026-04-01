#!/usr/bin/env python3
"""
NSW Property Tracker — Run all scrapers and push to server.

Usage:
  python3 run_all.py                  # scrape Domain + REA, push to default server
  python3 run_all.py --dry-run        # print results without pushing
  python3 run_all.py --source domain  # Domain only
  python3 run_all.py --source rea     # REA only
  python3 run_all.py --server URL     # custom tracker server

Recommended: run daily from a residential IP (laptop, home server)
  0 7 * * * cd ~/trackers && python3 run_all.py >> ~/logs/tracker.log 2>&1
"""

import argparse
import json
import sys
from datetime import datetime

DEFAULT_SERVER = "https://tmntech.ddns.net/tracker/api/ingest"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source",  choices=["domain","rea","all"], default="all")
    parser.add_argument("--server",  default=DEFAULT_SERVER)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] NSW Property Tracker — run_all.py")
    print(f"  Server: {args.server}")
    print(f"  Mode:   {'DRY RUN' if args.dry_run else 'LIVE'}\n")

    all_listings = []
    total_stats  = {"inserted": 0, "updated": 0, "skipped": 0, "deduped": 0}

    # ── Domain ────────────────────────────────────────────────────────────────
    if args.source in ("domain", "all"):
        print("=" * 50)
        print("  Source 1: Domain.com.au API")
        print("=" * 50)
        try:
            from domain_scraper import scrape as scrape_domain, push
            domain_listings = scrape_domain()
            print(f"  → {len(domain_listings)} listings from Domain")

            if not args.dry_run and domain_listings:
                push(domain_listings, args.server)
            else:
                all_listings.extend(domain_listings)

        except Exception as e:
            print(f"  [!] Domain scraper error: {e}", file=sys.stderr)

    # ── REA ───────────────────────────────────────────────────────────────────
    if args.source in ("rea", "all"):
        print()
        print("=" * 50)
        print("  Source 2: realestate.com.au")
        print("=" * 50)
        try:
            from rea_scraper import scrape as scrape_rea, push
            rea_listings = scrape_rea()
            print(f"  → {len(rea_listings)} listings from REA")

            if not args.dry_run and rea_listings:
                push(rea_listings, args.server)
            else:
                all_listings.extend(rea_listings)

        except Exception as e:
            print(f"  [!] REA scraper error: {e}", file=sys.stderr)

    # ── Dry run summary ───────────────────────────────────────────────────────
    if args.dry_run and all_listings:
        print(f"\n--- DRY RUN: {len(all_listings)} total listings ---")
        # Show source breakdown
        from collections import Counter
        sources = Counter(p["source"] for p in all_listings)
        for src, count in sources.items():
            print(f"  {src}: {count}")

        # Show 3 samples
        print("\nSamples:")
        for p in all_listings[:3]:
            p_clean = {k: v for k, v in p.items() if k != "raw"}
            print(json.dumps(p_clean, indent=2, ensure_ascii=False))

    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Done.")


if __name__ == "__main__":
    main()
