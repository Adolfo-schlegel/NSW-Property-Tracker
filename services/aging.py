"""
services/aging.py — Aging logic and property filtering
"""
from datetime import date
from typing import Optional

import config
from db.models import get_aged_properties, get_all_active, get_stats


def get_stale_listings(
    days:         int = None,
    listing_type: str = None,
    suburb:       str = None,
    max_price:    int = None,
    min_price:    int = None,
    min_beds:     int = None,
    prop_type:    str = None,
) -> list[dict]:
    """
    Return active listings older than `days` with optional filters.
    Primary query uses days_on_market virtual column.
    """
    days = days or config.AGING_DAYS
    listings = get_aged_properties(days=days, listing_type=listing_type)

    # Apply additional filters
    if suburb:
        listings = [l for l in listings if suburb.lower() in (l.get("suburb") or "").lower()]
    if max_price:
        listings = [l for l in listings if l.get("price_value") and l["price_value"] <= max_price]
    if min_price:
        listings = [l for l in listings if l.get("price_value") and l["price_value"] >= min_price]
    if min_beds:
        listings = [l for l in listings if l.get("bedrooms") and l["bedrooms"] >= min_beds]
    if prop_type:
        listings = [l for l in listings if (l.get("property_type") or "").lower() == prop_type.lower()]

    return listings


def get_summary_stats() -> dict:
    """Return dashboard stats."""
    base = get_stats()
    aged_30 = len(get_aged_properties(days=30))
    aged_60 = len(get_aged_properties(days=60))
    aged_90 = len(get_aged_properties(days=90))
    return {
        **base,
        "aged_30_days": aged_30,
        "aged_60_days": aged_60,
        "aged_90_days": aged_90,
    }


def format_listing_short(listing: dict) -> str:
    """Format a single listing for Telegram message."""
    addr  = listing.get("address") or "Unknown address"
    price = listing.get("price") or "Price undisclosed"
    days  = listing.get("days_on_market") or "?"
    url   = listing.get("url") or ""

    beds = listing.get("bedrooms")
    beds_str = f"{beds}🛏 " if beds else ""

    suburb = listing.get("suburb") or ""
    suburb_str = f" | {suburb}" if suburb else ""

    return (
        f"📍 *{addr}*{suburb_str}\n"
        f"💰 {price} | 📅 {days} days on market | {beds_str}\n"
        f"🔗 {url}"
    )


def format_aged_report(listings: list[dict], days: int = None) -> str:
    """Format a full aged listings report for Telegram."""
    days = days or config.AGING_DAYS
    if not listings:
        return f"✅ No properties older than {days} days on market right now."

    lines = [f"🏠 *{len(listings)} properties ≥ {days} days on market*\n"]
    for i, l in enumerate(listings[:20], 1):  # cap at 20 for readability
        lines.append(f"{i}. {format_listing_short(l)}\n")

    if len(listings) > 20:
        lines.append(f"_... and {len(listings) - 20} more. Check Google Sheets for full list._")

    return "\n".join(lines)
