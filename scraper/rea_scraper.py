#!/usr/bin/env python3
"""
NSW Property Tracker — realestate.com.au Scraper
=================================================
Scrapes REA via their internal search API.
⚠️  Must run from a residential IP (home machine / laptop).
    This server's IP is rate-limited by REA/Cloudflare.

Setup:
  pip install requests playwright
  playwright install chromium  # only needed if HTML mode

Usage:
  python3 rea_scraper.py                      # scrape & print
  python3 rea_scraper.py --push               # scrape & push to tracker server
  python3 rea_scraper.py --push --server URL  # custom server
  python3 rea_scraper.py --dry-run            # show first 3 results

Run from cron (on your laptop/home server):
  0 7 * * * cd /path/to/scraper && python3 rea_scraper.py --push
"""

import requests
import json
import time
import argparse
import sys
import random
from datetime import date, datetime
from urllib.parse import urlencode

# ── Config ──────────────────────────────────────────────────────────────────
POSTCODES      = ["2110", "2111", "2112", "2113", "2114", "2115"]
PAGE_SIZE      = 25      # REA max per page
DEFAULT_SERVER = "https://tmntech.ddns.net/tracker/api/ingest"
DELAY_BETWEEN  = (1.5, 3.0)  # random sleep seconds between requests

# Suburb → postcode mapping for REA search
SUBURB_POSTCODE_MAP = {
    "2110": ["hunters-hill", "woolwich", "henley"],
    "2111": ["gladesville", "meadowbank", "west-ryde"],
    "2112": ["ryde"],
    "2113": ["eastwood", "north-ryde"],
    "2114": ["putney", "meadowbank", "shepherd-bay"],
    "2115": ["ermington", "rydalmere"],
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/122.0.0.0 Safari/537.36",
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-AU,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer":         "https://www.realestate.com.au/",
    "Origin":          "https://www.realestate.com.au",
    "Cache-Control":   "no-cache",
}

# ── REA internal search endpoint ─────────────────────────────────────────────
REA_SEARCH_URL = "https://services.realestate.com.au/services/listings/search"

def build_rea_query(location: str, channel: str = "buy", page: int = 1) -> dict:
    return {
        "channel":    channel,
        "filters": {
            "surroundingSuburbs": False,
            "excludeNoSalePrice": False,
            "platform": "web",
            "nonDisplay": False,
        },
        "localities": [{"searchLocation": location}],
        "pageSize":   PAGE_SIZE,
        "pageNumber": page,
        "sort":       {"sortKey": "dateListed", "direction": "descending"},
    }

# ── Normalize a REA listing ──────────────────────────────────────────────────
def normalize_rea(raw: dict) -> dict | None:
    listing = raw.get("listingModel") or raw
    addr    = listing.get("address") or {}

    street_no   = addr.get("streetNumber") or ""
    street_name = addr.get("street")       or ""
    suburb      = addr.get("suburb")       or listing.get("suburb") or ""
    postcode    = addr.get("postcode")     or listing.get("postcode") or ""
    state       = addr.get("state")        or "NSW"

    # Only keep target postcodes
    if postcode not in POSTCODES:
        return None

    address = f"{street_no} {street_name}, {suburb} {state} {postcode}".strip(", ")

    price_str = listing.get("price") or raw.get("price") or ""
    price_val = 0
    if isinstance(price_str, str):
        import re
        nums = re.findall(r"\d+", price_str.replace(",", ""))
        if nums:
            price_val = int(nums[0])

    date_listed = listing.get("dateListed") or listing.get("dateFirstListed") or ""
    if date_listed:
        try:
            date_listed = datetime.fromisoformat(date_listed.replace("Z", "")).strftime("%Y-%m-%d")
        except Exception:
            date_listed = date_listed[:10]

    today          = date.today().isoformat()
    first_seen     = date_listed or today
    days_on_market = (date.today() - date.fromisoformat(first_seen)).days if first_seen else 0

    features   = listing.get("features") or {}
    prop_type  = listing.get("propertyType") or raw.get("propertyType") or ""
    rea_id     = str(listing.get("id") or raw.get("id") or "")
    url        = listing.get("url") or raw.get("url") or ""
    if url and not url.startswith("http"):
        url = f"https://www.realestate.com.au{url}"

    if not suburb or not rea_id:
        return None

    return {
        "source_id":      f"rea_{rea_id}",
        "source":         "realestate",
        "address":        address,
        "street_no":      street_no,
        "street_name":    street_name,
        "suburb":         suburb,
        "state":          state,
        "postcode":       postcode,
        "listing_type":   "Sale",
        "property_type":  prop_type,
        "price":          price_str,
        "price_value":    price_val,
        "bedrooms":       features.get("beds")    or listing.get("bedrooms")  or 0,
        "bathrooms":      features.get("baths")   or listing.get("bathrooms") or 0,
        "carspaces":      features.get("parking") or listing.get("carSpaces") or 0,
        "land_size":      listing.get("landArea") or 0,
        "first_seen":     first_seen,
        "last_seen":      today,
        "days_on_market": days_on_market,
        "url":            url,
        "raw":            json.dumps(listing)[:2000],
    }

# ── Scrape all suburbs ───────────────────────────────────────────────────────
def scrape() -> list[dict]:
    results  = []
    seen_ids = set()
    session  = requests.Session()
    session.headers.update(HEADERS)

    for postcode, suburbs in SUBURB_POSTCODE_MAP.items():
        for suburb in suburbs:
            location = f"{suburb}, nsw {postcode}"
            page     = 1
            print(f"  Scraping: {location}")

            while True:
                query = build_rea_query(location, page=page)
                params = {"query": json.dumps(query)}

                try:
                    resp = session.get(REA_SEARCH_URL, params=params, timeout=20)
                except requests.RequestException as e:
                    print(f"    [!] Request error: {e}", file=sys.stderr)
                    break

                if resp.status_code == 429:
                    wait = float(resp.headers.get("Retry-After", 10))
                    print(f"    [!] 429 rate limit — waiting {wait}s...", file=sys.stderr)
                    time.sleep(wait)
                    continue

                if resp.status_code != 200:
                    print(f"    [!] HTTP {resp.status_code}: {resp.text[:200]}", file=sys.stderr)
                    break

                try:
                    data = resp.json()
                except Exception:
                    print(f"    [!] Non-JSON response", file=sys.stderr)
                    break

                # Navigate response structure
                listings_raw = (
                    data.get("tieredResults", [{}])[0].get("results", []) or
                    data.get("results", []) or
                    data.get("listings", []) or
                    []
                )

                if not listings_raw:
                    break

                for raw in listings_raw:
                    normalized = normalize_rea(raw)
                    if normalized and normalized["source_id"] not in seen_ids:
                        seen_ids.add(normalized["source_id"])
                        results.append(normalized)

                print(f"    page {page}: {len(listings_raw)} listings (total: {len(results)})")

                if len(listings_raw) < PAGE_SIZE:
                    break
                page += 1

                # Polite delay
                time.sleep(random.uniform(*DELAY_BETWEEN))

            time.sleep(random.uniform(*DELAY_BETWEEN))

    print(f"\n✓ REA: {len(results)} unique listings scraped")
    return results

# ── Push to tracker server ───────────────────────────────────────────────────
def push(listings: list[dict], server_url: str) -> None:
    if not listings:
        print("Nothing to push.")
        return
    try:
        resp = requests.post(
            server_url,
            json={"source": "realestate", "listings": listings},
            timeout=30,
        )
        if resp.status_code == 200:
            result = resp.json()
            print(f"✓ Server accepted: {result.get('inserted',0)} new, {result.get('updated',0)} updated, {result.get('skipped',0)} duplicates")
        else:
            print(f"[!] Server returned {resp.status_code}: {resp.text[:300]}")
    except requests.RequestException as e:
        print(f"[!] Push failed: {e}")

# ── CLI ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape realestate.com.au listings")
    parser.add_argument("--push",    action="store_true", help="Push results to tracker server")
    parser.add_argument("--server",  default=DEFAULT_SERVER, help=f"Server ingest URL")
    parser.add_argument("--dry-run", action="store_true", help="Print first 3 results and exit")
    args = parser.parse_args()

    print(f"[REA Scraper] Postcodes: {', '.join(POSTCODES)}")
    print(f"[REA Scraper] Date: {date.today().isoformat()}")
    print(f"[REA Scraper] ⚠  Run from residential IP for best results\n")

    listings = scrape()

    if args.dry_run:
        print("\n--- Sample (first 3) ---")
        for p in listings[:3]:
            p_clean = {k: v for k, v in p.items() if k != "raw"}
            print(json.dumps(p_clean, indent=2, ensure_ascii=False))
        sys.exit(0)

    if args.push:
        print(f"\nPushing to {args.server}...")
        push(listings, args.server)
    else:
        print("\nTip: run with --push to send to the tracker server")
        if listings:
            p = {k: v for k, v in listings[0].items() if k != "raw"}
            print(json.dumps(p, indent=2, ensure_ascii=False))
