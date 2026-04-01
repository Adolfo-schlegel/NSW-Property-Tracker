#!/usr/bin/env python3
"""
REA Push Client — run from your laptop/home machine
====================================================
Scrapes realestate.com.au and sends results to the tracker server.

Setup (one time):
  pip install requests

Usage:
  python3 rea_push_client.py           # scrape and push
  python3 rea_push_client.py --dry-run # scrape only, don't push

Add to laptop crontab (crontab -e):
  0 7 * * * cd ~/Downloads && python3 rea_push_client.py >> ~/rea_tracker.log 2>&1
"""

import requests, json, time, random, sys, re
from datetime import date, datetime

SERVER_URL = "https://tmntech.ddns.net/tracker/api/ingest"
POSTCODES  = ["2110", "2111", "2112", "2113", "2114", "2115"]
PAGE_SIZE  = 25
DELAY      = (1.5, 3.5)

SUBURB_MAP = {
    "2110": ["hunters hill", "woolwich"],
    "2111": ["gladesville", "meadowbank", "west ryde"],
    "2112": ["ryde"],
    "2113": ["eastwood", "north ryde"],
    "2114": ["putney", "shepherd bay", "ermington"],
    "2115": ["rydalmere"],
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "en-AU,en;q=0.9",
    "Referer": "https://www.realestate.com.au/",
}

def normalize_rea(raw):
    lm     = raw.get("listingModel") or raw
    addr   = lm.get("address") or {}
    sno    = addr.get("streetNumber","")
    sname  = addr.get("street","")
    suburb = addr.get("suburb","")
    pc     = addr.get("postcode","")
    state  = addr.get("state","NSW")
    if pc not in POSTCODES: return None
    address = f"{sno} {sname}, {suburb} {state} {pc}".strip(", ")
    price_s = lm.get("price") or raw.get("price") or ""
    price_v = 0
    if isinstance(price_s, str):
        nums = re.findall(r"\d+", price_s.replace(",",""))
        price_v = int(nums[0]) if nums else 0
    dl = lm.get("dateListed","")
    try: dl = datetime.fromisoformat(dl.replace("Z","")).strftime("%Y-%m-%d") if dl else ""
    except: dl = dl[:10] if dl else ""
    today = date.today().isoformat()
    fs    = dl or today
    dom   = (date.today() - date.fromisoformat(fs)).days
    feat  = lm.get("features") or {}
    rid   = str(lm.get("id") or raw.get("id",""))
    url   = lm.get("url") or raw.get("url","")
    if url and not url.startswith("http"): url = f"https://www.realestate.com.au{url}"
    if not rid: return None
    return {
        "source_id": f"rea_{rid}", "source": "realestate",
        "address": address, "street_no": sno, "street_name": sname,
        "suburb": suburb, "state": state, "postcode": pc,
        "listing_type": "Sale", "property_type": lm.get("propertyType",""),
        "price": price_s, "price_value": price_v,
        "bedrooms":  feat.get("beds",0) or 0,
        "bathrooms": feat.get("baths",0) or 0,
        "carspaces": feat.get("parking",0) or 0,
        "first_seen": fs, "last_seen": today,
        "days_on_market": dom, "url": url, "raw": "",
    }

def scrape():
    results, seen = [], set()
    session = requests.Session()
    session.headers.update(HEADERS)
    for pc, suburbs in SUBURB_MAP.items():
        for suburb in suburbs:
            loc  = f"{suburb}, nsw {pc}"
            page = 1
            print(f"  {loc}...")
            while True:
                q = json.dumps({"filters":{"channel":"buy"},"localities":[{"searchLocation":loc}],"pageSize":PAGE_SIZE,"pageNumber":page})
                try:
                    r = session.get("https://services.realestate.com.au/services/listings/search", params={"query":q}, timeout=20)
                except Exception as e:
                    print(f"    Error: {e}"); break
                if r.status_code == 429:
                    wait = float(r.headers.get("Retry-After",15))
                    print(f"    429 — wait {wait}s"); time.sleep(wait); continue
                if r.status_code != 200:
                    print(f"    HTTP {r.status_code}"); break
                try: data = r.json()
                except: print("    Non-JSON"); break
                raw_list = (data.get("tieredResults",[{}])[0].get("results",[]) or data.get("results",[]))
                if not raw_list: break
                for item in raw_list:
                    n = normalize_rea(item)
                    if n and n["source_id"] not in seen:
                        seen.add(n["source_id"]); results.append(n)
                print(f"    page {page}: {len(raw_list)} (total {len(results)})")
                if len(raw_list) < PAGE_SIZE: break
                page += 1
                time.sleep(random.uniform(*DELAY))
            time.sleep(random.uniform(*DELAY))
    return results

def push(listings):
    r = requests.post(SERVER_URL, json={"source":"realestate","listings":listings}, timeout=30)
    if r.status_code == 200:
        d = r.json()
        print(f"✓ Server: {d.get('inserted',0)} new, {d.get('updated',0)} updated, {d.get('deduped',0)} deduped")
    else:
        print(f"✗ Server {r.status_code}: {r.text[:200]}")

if __name__ == "__main__":
    dry = "--dry-run" in sys.argv
    print(f"[REA Push Client] {date.today()} — {'DRY RUN' if dry else 'LIVE'}")
    listings = scrape()
    print(f"\nTotal: {len(listings)} listings")
    if not dry and listings:
        push(listings)
    elif dry and listings:
        print(json.dumps({k:v for k,v in listings[0].items() if k!="raw"}, indent=2))
