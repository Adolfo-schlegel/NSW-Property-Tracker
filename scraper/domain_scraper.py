#!/usr/bin/env python3
"""
NSW Property Tracker — Domain.com.au Scraper
============================================
Uses the Domain Developer API (api.domain.com.au).
Requires: "Listings Management → Sandbox" package activated in your Domain project.

Setup:
  pip install requests

Usage:
  python3 domain_scraper.py                      # scrape & print
  python3 domain_scraper.py --push               # scrape & push to tracker server
  python3 domain_scraper.py --push --server URL  # custom server URL
"""

import requests
import json
import argparse
import sys
from datetime import date, datetime

# ── Config ──────────────────────────────────────────────────────────────────
API_KEY      = os.environ.get("DOMAIN_API_KEY", "")
API_BASE     = "https://api.domain.com.au/v1"
POSTCODES    = ["2110", "2111", "2112", "2113", "2114", "2115"]
PAGE_SIZE    = 200   # max per request
DEFAULT_SERVER = "https://tmntech.ddns.net/tracker/api/ingest"

HEADERS = {
    "X-Api-Key": API_KEY,
    "Content-Type": "application/json",
    "Accept": "application/json",
}

# ── Search payload ───────────────────────────────────────────────────────────
def build_payload(postcode: str, page: int = 1) -> dict:
    return {
        "listingType": "Sale",
        "locations": [{"state": "NSW", "postCode": postcode}],
        "pageSize": PAGE_SIZE,
        "page": page,
        "sort": {"sortKey": "dateUpdated", "direction": "Descending"},
        "propertyTypes": ["House", "ApartmentUnitFlat", "Townhouse", "Villa", "Land", "Rural", "BlockOfUnits"],
    }

# ── Normalize a single Domain listing ───────────────────────────────────────
def normalize(raw: dict) -> dict | None:
    listing = raw.get("listing", raw)   # API wraps in "listing" key sometimes
    addr    = listing.get("addressParts") or listing.get("address") or {}
    
    if isinstance(addr, str):
        addr = {"displayAddress": addr}

    street_no   = addr.get("streetNumber") or addr.get("streetNo") or ""
    street_name = addr.get("street")       or addr.get("streetName") or ""
    street_type = addr.get("streetType")   or ""
    suburb      = addr.get("suburb")       or listing.get("suburb") or ""
    postcode    = addr.get("postcode")     or listing.get("postcode") or ""
    state       = addr.get("state")        or "NSW"

    # Full address string
    address = f"{street_no} {street_name} {street_type}, {suburb} {state} {postcode}".strip(", ")

    # Price
    price_details = listing.get("priceDetails") or {}
    price_str     = price_details.get("displayPrice") or listing.get("price") or ""
    price_val     = price_details.get("price") or 0

    # Dates
    date_listed = listing.get("dateListed") or listing.get("dateFirstListed") or ""
    if date_listed:
        try:
            date_listed = datetime.fromisoformat(date_listed.replace("Z","")).strftime("%Y-%m-%d")
        except Exception:
            date_listed = date_listed[:10]

    today     = date.today().isoformat()
    first_seen = date_listed or today
    days_on_market = (date.today() - date.fromisoformat(first_seen)).days if first_seen else 0

    features = listing.get("features") or {}
    prop_type = listing.get("propertyTypes", [None])[0] if isinstance(listing.get("propertyTypes"), list) else listing.get("propertyType") or ""

    domain_id = str(listing.get("id") or listing.get("listingId") or "")
    url = f"https://www.domain.com.au/{domain_id}" if domain_id else listing.get("url") or ""

    if not suburb or not postcode:
        return None

    return {
        "source_id":      f"domain_{domain_id}",
        "source":         "domain",
        "address":        address,
        "street_no":      street_no,
        "street_name":    f"{street_name} {street_type}".strip(),
        "suburb":         suburb,
        "state":          state,
        "postcode":       postcode,
        "listing_type":   listing.get("saleMethod") or "Sale",
        "property_type":  prop_type,
        "price":          price_str,
        "price_value":    price_val,
        "bedrooms":       features.get("numBedrooms") or listing.get("bedrooms") or 0,
        "bathrooms":      features.get("numBathrooms") or listing.get("bathrooms") or 0,
        "carspaces":      features.get("numCarSpaces") or listing.get("carSpaces") or 0,
        "land_size":      listing.get("landArea") or 0,
        "first_seen":     first_seen,
        "last_seen":      today,
        "days_on_market": days_on_market,
        "url":            url,
        "raw":            json.dumps(listing)[:2000],
    }

# ── Scrape all postcodes ─────────────────────────────────────────────────────
def scrape() -> list[dict]:
    results = []
    seen_ids = set()

    for postcode in POSTCODES:
        page = 1
        while True:
            payload = build_payload(postcode, page)
            try:
                resp = requests.post(
                    f"{API_BASE}/listings/residential/_search",
                    headers=HEADERS,
                    json=payload,
                    timeout=30,
                )
            except requests.RequestException as e:
                print(f"  [!] Request error for {postcode} p{page}: {e}", file=sys.stderr)
                break

            if resp.status_code == 403:
                print(f"  [!] 403 — Domain API package not activated. Add 'Listings Management → Sandbox' at developer.domain.com.au", file=sys.stderr)
                return results

            if resp.status_code != 200:
                print(f"  [!] HTTP {resp.status_code} for postcode {postcode}: {resp.text[:200]}", file=sys.stderr)
                break

            data = resp.json()
            if not isinstance(data, list):
                data = data.get("listings") or data.get("results") or []

            if not data:
                break

            for raw in data:
                normalized = normalize(raw)
                if normalized and normalized["source_id"] not in seen_ids:
                    seen_ids.add(normalized["source_id"])
                    results.append(normalized)

            print(f"  postcode {postcode} page {page}: {len(data)} listings (total so far: {len(results)})")

            if len(data) < PAGE_SIZE:
                break
            page += 1

    print(f"\n✓ Domain: {len(results)} unique listings scraped across postcodes {', '.join(POSTCODES)}")
    return results

# ── Push to tracker server ───────────────────────────────────────────────────
def push(listings: list[dict], server_url: str) -> None:
    if not listings:
        print("Nothing to push.")
        return
    try:
        resp = requests.post(
            server_url,
            json={"source": "domain", "listings": listings},
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
    parser = argparse.ArgumentParser(description="Scrape Domain.com.au listings")
    parser.add_argument("--push",   action="store_true", help="Push results to tracker server")
    parser.add_argument("--server", default=DEFAULT_SERVER, help=f"Server ingest URL (default: {DEFAULT_SERVER})")
    parser.add_argument("--dry-run", action="store_true", help="Print first 3 results and exit")
    args = parser.parse_args()

    print(f"[Domain Scraper] Postcodes: {', '.join(POSTCODES)}")
    print(f"[Domain Scraper] Date: {date.today().isoformat()}\n")

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
        print("Sample listing:")
        if listings:
            p = {k: v for k, v in listings[0].items() if k != "raw"}
            print(json.dumps(p, indent=2, ensure_ascii=False))
