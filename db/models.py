"""
db/models.py — SQLite schema + all DB operations
"""
import sqlite3
import logging
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import config

logger = logging.getLogger(__name__)


@contextmanager
def get_conn():
    """Thread-safe SQLite connection."""
    Path(config.DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(config.DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    """Create tables if not exist."""
    with get_conn() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS properties (
                id          TEXT PRIMARY KEY,
                source      TEXT NOT NULL DEFAULT 'domain',
                url         TEXT NOT NULL,
                address     TEXT,
                suburb      TEXT,
                state       TEXT DEFAULT 'NSW',
                postcode    TEXT,
                price       TEXT,
                price_value INTEGER,       -- parsed numeric price for filtering
                bedrooms    INTEGER,
                bathrooms   INTEGER,
                car_spaces  INTEGER,
                property_type TEXT,        -- house, apartment, townhouse, etc.
                agent       TEXT,
                agency      TEXT,
                listing_type TEXT DEFAULT 'sale',   -- sale | rent
                first_seen  DATE NOT NULL,
                last_seen   DATE NOT NULL,
                status      TEXT NOT NULL DEFAULT 'active',  -- active | inactive
                -- days_on_market calculated at query time (see get_aged_properties)
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_status       ON properties(status);
            CREATE INDEX IF NOT EXISTS idx_first_seen   ON properties(first_seen);
            CREATE INDEX IF NOT EXISTS idx_suburb       ON properties(suburb);
            CREATE INDEX IF NOT EXISTS idx_listing_type ON properties(listing_type);

            -- Track price changes over time
            CREATE TABLE IF NOT EXISTS price_history (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                property_id TEXT NOT NULL REFERENCES properties(id),
                price       TEXT,
                price_value INTEGER,
                recorded_at DATE NOT NULL DEFAULT CURRENT_DATE
            );

            CREATE INDEX IF NOT EXISTS idx_ph_property ON price_history(property_id);

            -- Scrape run log
            CREATE TABLE IF NOT EXISTS scrape_runs (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                source      TEXT NOT NULL,
                started_at  TIMESTAMP NOT NULL,
                finished_at TIMESTAMP,
                listings_found  INTEGER DEFAULT 0,
                listings_new    INTEGER DEFAULT 0,
                listings_updated INTEGER DEFAULT 0,
                listings_removed INTEGER DEFAULT 0,
                error       TEXT
            );
        """)
    logger.info("DB initialised at %s", config.DB_PATH)


# ── Property operations ───────────────────────────────────────────────────────

def upsert_property(listing: dict, today: date = None) -> str:
    """
    Insert new property or update last_seen.
    Returns: 'new' | 'updated' | 'price_changed'
    """
    today = today or date.today()
    pid   = listing["id"]

    with get_conn() as conn:
        existing = conn.execute(
            "SELECT id, price_value FROM properties WHERE id = ?", (pid,)
        ).fetchone()

        if not existing:
            conn.execute("""
                INSERT INTO properties
                  (id, source, url, address, suburb, postcode, price, price_value,
                   bedrooms, bathrooms, car_spaces, property_type, agent, agency,
                   listing_type, first_seen, last_seen, status)
                VALUES
                  (:id, :source, :url, :address, :suburb, :postcode, :price, :price_value,
                   :bedrooms, :bathrooms, :car_spaces, :property_type, :agent, :agency,
                   :listing_type, :first_seen, :last_seen, 'active')
            """, {
                "first_seen":   today,
                "last_seen":    today,
                "source":       listing.get("source", "domain"),
                "listing_type": listing.get("listing_type", config.LISTING_TYPE),
                **listing,
            })
            return "new"

        # Update last_seen
        conn.execute(
            "UPDATE properties SET last_seen=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
            (today, pid)
        )

        # Detect price change
        old_price = existing["price_value"]
        new_price = listing.get("price_value")
        if new_price and old_price and new_price != old_price:
            conn.execute(
                "UPDATE properties SET price=?, price_value=? WHERE id=?",
                (listing.get("price"), new_price, pid)
            )
            conn.execute(
                "INSERT INTO price_history (property_id, price, price_value) VALUES (?,?,?)",
                (pid, listing.get("price"), new_price)
            )
            return "price_changed"

        return "updated"


def mark_inactive(active_ids: set, source: str, today: date = None):
    """Mark as inactive any active listings not in today's scrape."""
    today = today or date.today()
    with get_conn() as conn:
        conn.execute("""
            UPDATE properties
            SET status = 'inactive', updated_at = CURRENT_TIMESTAMP
            WHERE status = 'active'
              AND source = ?
              AND id NOT IN ({})
        """.format(",".join("?" * len(active_ids))),
            [source] + list(active_ids)
        )


_DAYS_ON_MARKET = "CAST((julianday('now') - julianday(first_seen)) AS INTEGER) AS days_on_market"


def get_aged_properties(days: int = None, listing_type: str = None) -> list[dict]:
    """Return active properties older than N days."""
    days = days or config.AGING_DAYS
    query = """
        SELECT *, {dom}
        FROM properties
        WHERE status = 'active'
          AND first_seen <= date('now', ?)
          {lt}
        ORDER BY first_seen ASC
    """.format(
        dom=_DAYS_ON_MARKET,
        lt="AND listing_type = ?" if listing_type else ""
    )
    params = [f"-{days} days"]
    if listing_type:
        params.append(listing_type)

    with get_conn() as conn:
        rows = conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]


def get_all_active(listing_type: str = None) -> list[dict]:
    """Return all active properties with days_on_market calculated."""
    query = """
        SELECT *, {dom}
        FROM properties
        WHERE status = 'active'
        {lt}
        ORDER BY first_seen ASC
    """.format(
        dom=_DAYS_ON_MARKET,
        lt="AND listing_type = ?" if listing_type else ""
    )
    with get_conn() as conn:
        rows = conn.execute(query, [listing_type] if listing_type else []).fetchall()
        return [dict(r) for r in rows]


def get_stats() -> dict:
    with get_conn() as conn:
        total   = conn.execute("SELECT COUNT(*) FROM properties WHERE status='active'").fetchone()[0]
        aged    = conn.execute(f"SELECT COUNT(*) FROM properties WHERE status='active' AND first_seen <= date('now','-{config.AGING_DAYS} days')").fetchone()[0]
        new_today = conn.execute("SELECT COUNT(*) FROM properties WHERE first_seen = date('now')").fetchone()[0]
        return {"total_active": total, "aged": aged, "new_today": new_today}


def log_run(source: str, started_at: datetime) -> int:
    with get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO scrape_runs (source, started_at) VALUES (?,?)",
            (source, started_at)
        )
        return cur.lastrowid


def finish_run(run_id: int, counts: dict, error: str = None):
    with get_conn() as conn:
        conn.execute("""
            UPDATE scrape_runs
            SET finished_at=CURRENT_TIMESTAMP,
                listings_found=?, listings_new=?, listings_updated=?,
                listings_removed=?, error=?
            WHERE id=?
        """, (counts.get("found", 0), counts.get("new", 0), counts.get("updated", 0),
              counts.get("removed", 0), error, run_id))
