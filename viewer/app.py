"""
NSW Property Tracker — Web Viewer + Ingest API
Run: python viewer/app.py
"""
import sys, os, json
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from flask import Flask, render_template, request, jsonify
import datetime

app = Flask(__name__, template_folder="templates")

# ── DB path (new unified DB from dedup.py) ───────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
DB_PATH  = os.path.join(BASE_DIR, "properties.db")

# Try new dedup DB first; fall back to legacy if needed
try:
    from scraper.dedup import get_db, ingest, query_listings, get_stats
    USE_DEDUP_DB = True
except ImportError:
    USE_DEDUP_DB = False


# ── Legacy DB fallback ───────────────────────────────────────────────────────
import sqlite3

def get_legacy_conn():
    legacy = os.path.join(BASE_DIR, "db", "properties.db")
    conn = sqlite3.connect(legacy)
    conn.row_factory = sqlite3.Row
    return conn

def legacy_query(filters: dict) -> list[dict]:
    clauses = ["1=1"]
    params  = []

    if q := filters.get("q"):
        clauses.append("(LOWER(address) LIKE ? OR LOWER(suburb) LIKE ?)")
        params += [f"%{q.lower()}%", f"%{q.lower()}%"]
    if pcs := filters.get("postcodes"):
        plist = [p.strip() for p in pcs.split(",") if p.strip()]
        clauses.append(f"postcode IN ({','.join('?'*len(plist))})")
        params += plist
    if ft := filters.get("type"):
        clauses.append("listing_type LIKE ?")
        params.append(f"%{ft}%")
    if fd := filters.get("from"):
        clauses.append("first_seen >= ?"); params.append(fd)
    if td := filters.get("to"):
        clauses.append("first_seen <= ?"); params.append(td)
    if beds := filters.get("beds"):
        clauses.append("bedrooms >= ?"); params.append(int(beds))
    if mp := filters.get("max_price"):
        try:
            clauses.append("price_value <= ?"); params.append(int(str(mp).replace(",","")))
        except ValueError: pass
    if aged := filters.get("aged"):
        clauses.append("days_on_market >= ?"); params.append(int(aged))

    sort_map = {"first_seen":"first_seen","days_on_market":"days_on_market",
                "price_value":"price_value","suburb":"suburb"}
    sc  = sort_map.get(filters.get("sort",""), "first_seen")
    sd  = "ASC" if filters.get("dir") == "ASC" else "DESC"
    sql = f"SELECT * FROM properties WHERE {' AND '.join(clauses)} ORDER BY {sc} {sd} LIMIT 500"

    with get_legacy_conn() as conn:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]

def legacy_stats() -> dict:
    today = datetime.date.today().isoformat()
    with get_legacy_conn() as conn:
        total   = conn.execute("SELECT COUNT(*) FROM properties").fetchone()[0]
        today_c = conn.execute("SELECT COUNT(*) FROM properties WHERE first_seen=?", (today,)).fetchone()[0]
        a30     = conn.execute("SELECT COUNT(*) FROM properties WHERE days_on_market>=30").fetchone()[0]
        a60     = conn.execute("SELECT COUNT(*) FROM properties WHERE days_on_market>=60").fetchone()[0]
    return {"total":total,"today":today_c,"aged_30":a30,"aged_60":a60}


# ── Routes ───────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    today   = datetime.date.today().isoformat()
    filters = dict(request.args)

    if USE_DEDUP_DB:
        try:
            listings = query_listings(DB_PATH, filters)
            stats    = get_stats(DB_PATH)
        except Exception as e:
            listings = legacy_query(filters)
            stats    = legacy_stats()
    else:
        listings = legacy_query(filters)
        stats    = legacy_stats()

    return render_template("index.html",
        listings=listings, stats=stats,
        filters=request.args, today=today
    )


@app.route("/api/stats")
def api_stats():
    if USE_DEDUP_DB:
        return jsonify(get_stats(DB_PATH))
    return jsonify(legacy_stats())


@app.route("/api/listings")
def api_listings_json():
    filters = dict(request.args)
    if USE_DEDUP_DB:
        return jsonify(query_listings(DB_PATH, filters))
    return jsonify(legacy_query(filters))


# ── Ingest endpoint (scrapers POST here) ─────────────────────────────────────
@app.route("/api/ingest", methods=["POST"])
def api_ingest():
    """
    Accepts JSON: {"source": "domain|realestate", "listings": [...]}
    Returns: {"inserted": N, "updated": N, "skipped": N, "deduped": N}
    """
    if not USE_DEDUP_DB:
        return jsonify({"error": "dedup module not available"}), 503

    try:
        body     = request.get_json(force=True)
        source   = body.get("source", "unknown")
        listings = body.get("listings", [])

        if not listings:
            return jsonify({"error": "no listings provided"}), 400

        # Tag source on each listing if missing
        for p in listings:
            if not p.get("source"):
                p["source"] = source

        stats = ingest(listings, DB_PATH)
        return jsonify(stats)

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("VIEWER_PORT", 8765))
    print(f"\n  NSW Property Tracker — http://localhost:{port}")
    print(f"  Ingest API  — POST http://localhost:{port}/api/ingest")
    print(f"  Using dedup DB: {USE_DEDUP_DB}\n")
    app.run(host="0.0.0.0", port=port, debug=False)
