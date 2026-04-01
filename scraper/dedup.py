#!/usr/bin/env python3
"""
Deduplication engine for NSW Property Tracker.
Merges listings from multiple sources (Domain + REA), removing true duplicates.

Strategy:
  1. Same source_id       → exact duplicate (skip)
  2. Same address + type  → cross-source duplicate (keep best, merge data)
  3. Similar address      → fuzzy match for near-duplicates (manual review flag)
"""

import sqlite3
import re
import json
from datetime import date
from difflib import SequenceMatcher


DB_PATH = "/root/.openclaw/workspace/nsw-property-tracker/properties.db"


# ── Normalise address for comparison ────────────────────────────────────────
STREET_TYPE_MAP = {
    "street": "st", "road": "rd", "avenue": "ave", "drive": "dr",
    "court": "ct", "crescent": "cr", "place": "pl", "close": "cl",
    "lane": "ln", "way": "wy", "parade": "pde", "terrace": "tce",
    "boulevard": "blvd", "grove": "gr", "circuit": "cct",
}

def normalise_address(address: str) -> str:
    if not address:
        return ""
    a = address.lower().strip()
    a = re.sub(r"[,./]", " ", a)
    a = re.sub(r"\s+", " ", a)
    for long, short in STREET_TYPE_MAP.items():
        a = re.sub(rf"\b{long}\b", short, a)
    # Remove unit/apartment prefix variations
    a = re.sub(r"\b(unit|apt|apartment|flat)\s*\d+\s*[/\\]?\s*", "", a)
    return a.strip()

def address_similarity(a1: str, a2: str) -> float:
    n1 = normalise_address(a1)
    n2 = normalise_address(a2)
    if n1 == n2:
        return 1.0
    return SequenceMatcher(None, n1, n2).ratio()


# ── DB schema ────────────────────────────────────────────────────────────────
SCHEMA = """
CREATE TABLE IF NOT EXISTS properties (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id       TEXT    UNIQUE NOT NULL,
    sources         TEXT    NOT NULL DEFAULT '[]',   -- JSON list of source names
    address         TEXT    NOT NULL,
    address_norm    TEXT    NOT NULL,
    street_no       TEXT,
    street_name     TEXT,
    suburb          TEXT,
    state           TEXT    DEFAULT 'NSW',
    postcode        TEXT,
    listing_type    TEXT,
    property_type   TEXT,
    price           TEXT,
    price_value     INTEGER DEFAULT 0,
    bedrooms        INTEGER DEFAULT 0,
    bathrooms       INTEGER DEFAULT 0,
    carspaces       INTEGER DEFAULT 0,
    land_size       REAL    DEFAULT 0,
    first_seen      TEXT,
    last_seen       TEXT,
    days_on_market  INTEGER DEFAULT 0,
    url             TEXT,
    canonical_url   TEXT,   -- best URL across sources
    is_duplicate    INTEGER DEFAULT 0,
    duplicate_of    TEXT,   -- source_id of canonical record
    confidence      REAL    DEFAULT 1.0,
    raw             TEXT
);

CREATE INDEX IF NOT EXISTS idx_address_norm ON properties(address_norm);
CREATE INDEX IF NOT EXISTS idx_postcode     ON properties(postcode);
CREATE INDEX IF NOT EXISTS idx_first_seen   ON properties(first_seen);
CREATE INDEX IF NOT EXISTS idx_source_id    ON properties(source_id);
"""


def get_db(db_path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    conn.commit()
    return conn


# ── Ingest a batch of listings with dedup ────────────────────────────────────
def ingest(listings: list[dict], db_path: str = DB_PATH) -> dict:
    conn    = get_db(db_path)
    cur     = conn.cursor()
    today   = date.today().isoformat()
    stats   = {"inserted": 0, "updated": 0, "skipped": 0, "deduped": 0}

    for p in listings:
        source_id    = p.get("source_id", "")
        source       = p.get("source", "unknown")
        address      = p.get("address", "")
        address_norm = normalise_address(address)

        # ── 1. Exact source_id match → update last_seen only ────────────────
        existing = cur.execute(
            "SELECT id, sources, days_on_market FROM properties WHERE source_id = ?",
            (source_id,)
        ).fetchone()

        if existing:
            cur.execute(
                "UPDATE properties SET last_seen=?, days_on_market=? WHERE source_id=?",
                (today, p.get("days_on_market", 0), source_id)
            )
            stats["skipped"] += 1
            continue

        # ── 2. Cross-source address match ───────────────────────────────────
        if address_norm:
            candidates = cur.execute(
                "SELECT source_id, address_norm, sources, url FROM properties WHERE postcode=? AND is_duplicate=0",
                (p.get("postcode", ""),)
            ).fetchall()

            best_match = None
            best_score = 0.0
            for row in candidates:
                score = address_similarity(address_norm, row["address_norm"])
                if score > best_score:
                    best_score = score
                    best_match = row

            if best_score >= 0.92 and best_match:
                # Merge: add this source to existing record's sources list
                existing_sources = json.loads(best_match["sources"] or "[]")
                if source not in existing_sources:
                    existing_sources.append(source)
                # Prefer Domain URL over REA
                canonical_url = best_match["url"]
                if source == "domain" and p.get("url"):
                    canonical_url = p["url"]

                cur.execute(
                    "UPDATE properties SET sources=?, last_seen=?, canonical_url=? WHERE source_id=?",
                    (json.dumps(existing_sources), today, canonical_url, best_match["source_id"])
                )
                # Mark this as a duplicate
                cur.execute(
                    """INSERT OR IGNORE INTO properties
                       (source_id, sources, address, address_norm, street_no, street_name,
                        suburb, state, postcode, listing_type, property_type, price, price_value,
                        bedrooms, bathrooms, carspaces, land_size, first_seen, last_seen,
                        days_on_market, url, canonical_url, is_duplicate, duplicate_of, confidence, raw)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,?,?,?)""",
                    (
                        source_id,
                        json.dumps([source]),
                        address, address_norm,
                        p.get("street_no",""), p.get("street_name",""),
                        p.get("suburb",""), p.get("state","NSW"), p.get("postcode",""),
                        p.get("listing_type","Sale"), p.get("property_type",""),
                        p.get("price",""), p.get("price_value",0),
                        p.get("bedrooms",0), p.get("bathrooms",0), p.get("carspaces",0),
                        p.get("land_size",0),
                        p.get("first_seen",today), today,
                        p.get("days_on_market",0),
                        p.get("url",""), p.get("url",""),
                        best_match["source_id"],
                        round(best_score, 3),
                        p.get("raw","")
                    )
                )
                stats["deduped"] += 1
                continue

        # ── 3. New listing — insert ─────────────────────────────────────────
        cur.execute(
            """INSERT OR IGNORE INTO properties
               (source_id, sources, address, address_norm, street_no, street_name,
                suburb, state, postcode, listing_type, property_type, price, price_value,
                bedrooms, bathrooms, carspaces, land_size, first_seen, last_seen,
                days_on_market, url, canonical_url, is_duplicate, duplicate_of, confidence, raw)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,NULL,1.0,?)""",
            (
                source_id,
                json.dumps([source]),
                address, address_norm,
                p.get("street_no",""), p.get("street_name",""),
                p.get("suburb",""), p.get("state","NSW"), p.get("postcode",""),
                p.get("listing_type","Sale"), p.get("property_type",""),
                p.get("price",""), p.get("price_value",0),
                p.get("bedrooms",0), p.get("bathrooms",0), p.get("carspaces",0),
                p.get("land_size",0),
                p.get("first_seen",today), today,
                p.get("days_on_market",0),
                p.get("url",""), p.get("url",""),
                p.get("raw","")
            )
        )
        if cur.rowcount:
            stats["inserted"] += 1
        else:
            stats["skipped"] += 1

    conn.commit()
    conn.close()
    return stats


# ── Query active (non-duplicate) listings ────────────────────────────────────
def query_listings(db_path: str = DB_PATH, filters: dict = None) -> list[dict]:
    conn    = get_db(db_path)
    cur     = conn.cursor()
    filters = filters or {}

    where  = ["is_duplicate = 0"]
    params = []

    if q := filters.get("q"):
        where.append("(address LIKE ? OR suburb LIKE ?)")
        params += [f"%{q}%", f"%{q}%"]

    if postcodes := filters.get("postcodes"):
        plist = [p.strip() for p in postcodes.split(",") if p.strip()]
        where.append(f"postcode IN ({','.join('?'*len(plist))})")
        params += plist

    if listing_type := filters.get("type"):
        where.append("listing_type LIKE ?")
        params.append(f"%{listing_type}%")

    if from_date := filters.get("from"):
        where.append("first_seen >= ?")
        params.append(from_date)

    if to_date := filters.get("to"):
        where.append("first_seen <= ?")
        params.append(to_date)

    if beds := filters.get("beds"):
        where.append("bedrooms >= ?")
        params.append(int(beds))

    if max_price := filters.get("max_price"):
        try:
            where.append("(price_value <= ? AND price_value > 0)")
            params.append(int(str(max_price).replace(",","")))
        except ValueError:
            pass

    if aged := filters.get("aged"):
        where.append("days_on_market >= ?")
        params.append(int(aged))

    sort_map = {
        "first_seen":     "first_seen",
        "days_on_market": "days_on_market",
        "price_value":    "price_value",
        "suburb":         "suburb",
    }
    sort_col = sort_map.get(filters.get("sort",""), "first_seen")
    sort_dir = "ASC" if filters.get("dir","DESC") == "ASC" else "DESC"

    sql = f"""
        SELECT * FROM properties
        WHERE {' AND '.join(where)}
        ORDER BY {sort_col} {sort_dir}
        LIMIT 500
    """
    rows = cur.execute(sql, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ── Stats ────────────────────────────────────────────────────────────────────
def get_stats(db_path: str = DB_PATH) -> dict:
    conn  = get_db(db_path)
    cur   = conn.cursor()
    today = date.today().isoformat()

    total    = cur.execute("SELECT COUNT(*) FROM properties WHERE is_duplicate=0").fetchone()[0]
    today_c  = cur.execute("SELECT COUNT(*) FROM properties WHERE is_duplicate=0 AND first_seen=?", (today,)).fetchone()[0]
    aged_30  = cur.execute("SELECT COUNT(*) FROM properties WHERE is_duplicate=0 AND days_on_market>=30").fetchone()[0]
    aged_60  = cur.execute("SELECT COUNT(*) FROM properties WHERE is_duplicate=0 AND days_on_market>=60").fetchone()[0]
    deduped  = cur.execute("SELECT COUNT(*) FROM properties WHERE is_duplicate=1").fetchone()[0]
    sources  = cur.execute("SELECT DISTINCT sources FROM properties WHERE is_duplicate=0").fetchall()
    conn.close()

    return {
        "total": total, "today": today_c,
        "aged_30": aged_30, "aged_60": aged_60,
        "deduped_count": deduped,
    }


if __name__ == "__main__":
    # Quick test
    db = get_db()
    stats = get_stats()
    print("DB stats:", json.dumps(stats, indent=2))
