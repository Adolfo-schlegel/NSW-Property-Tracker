"""
services/sheets.py — Google Sheets sync via gspread
Requires: SHEETS_CREDENTIALS_FILE (service account JSON) + SHEETS_SPREADSHEET_ID
"""
import logging
from datetime import date
from typing import Optional

import config

logger = logging.getLogger(__name__)


def _get_client():
    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError:
        raise RuntimeError("gspread not installed. Run: pip install gspread google-auth")

    creds = Credentials.from_service_account_file(
        config.SHEETS_CREDENTIALS_FILE,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds)


def sync_all_properties(listings: list[dict]) -> bool:
    """Overwrite the all_properties sheet with current active listings."""
    if not config.SHEETS_ENABLED:
        logger.debug("sheets: disabled — skipping sync")
        return False
    if not listings:
        logger.info("sheets: no listings to sync")
        return True

    try:
        client = _get_client()
        ss     = client.open_by_key(config.SHEETS_SPREADSHEET_ID)
        ws     = _get_or_create_sheet(ss, config.SHEETS_ALL_TAB)

        headers = [
            "ID", "Source", "Address", "Suburb", "Postcode", "Price",
            "Bedrooms", "Bathrooms", "Car Spaces", "Type",
            "First Seen", "Last Seen", "Days on Market", "Status", "URL",
        ]

        rows = [headers]
        for l in listings:
            rows.append([
                l.get("id", ""),
                l.get("source", ""),
                l.get("address", ""),
                l.get("suburb", ""),
                l.get("postcode", ""),
                l.get("price", ""),
                l.get("bedrooms", ""),
                l.get("bathrooms", ""),
                l.get("car_spaces", ""),
                l.get("property_type", ""),
                str(l.get("first_seen", "")),
                str(l.get("last_seen", "")),
                l.get("days_on_market", ""),
                l.get("status", ""),
                l.get("url", ""),
            ])

        ws.clear()
        ws.update(rows, value_input_option="USER_ENTERED")
        logger.info("sheets: synced %d properties to '%s'", len(listings), config.SHEETS_ALL_TAB)
        return True

    except Exception as e:
        logger.error("sheets: sync_all failed: %s", e)
        return False


def sync_aged_properties(aged_listings: list[dict]) -> bool:
    """Overwrite the aged_60_days sheet."""
    if not config.SHEETS_ENABLED:
        return False

    try:
        client = _get_client()
        ss     = client.open_by_key(config.SHEETS_SPREADSHEET_ID)
        ws     = _get_or_create_sheet(ss, config.SHEETS_AGED_TAB)

        headers = [
            "ID", "Source", "Address", "Suburb", "Price",
            "Days on Market", "First Seen", "Bedrooms", "URL",
        ]
        rows = [headers]
        for l in aged_listings:
            rows.append([
                l.get("id", ""),
                l.get("source", ""),
                l.get("address", ""),
                l.get("suburb", ""),
                l.get("price", ""),
                l.get("days_on_market", ""),
                str(l.get("first_seen", "")),
                l.get("bedrooms", ""),
                l.get("url", ""),
            ])

        ws.clear()
        ws.update(rows, value_input_option="USER_ENTERED")

        # Highlight rows — color aged_90 in red, aged_60 in orange
        _highlight_aged_rows(ws, aged_listings)

        logger.info("sheets: synced %d aged properties to '%s'",
                    len(aged_listings), config.SHEETS_AGED_TAB)
        return True

    except Exception as e:
        logger.error("sheets: sync_aged failed: %s", e)
        return False


def _highlight_aged_rows(ws, listings: list[dict]):
    """Color rows by days on market — cosmetic, non-critical."""
    try:
        import gspread.utils
        for i, listing in enumerate(listings, start=2):  # row 1 = header
            days = listing.get("days_on_market") or 0
            if days >= 90:
                color = {"red": 0.96, "green": 0.80, "blue": 0.80}   # light red
            elif days >= 60:
                color = {"red": 1.0,  "green": 0.93, "blue": 0.82}   # light orange
            else:
                continue
            ws.format(f"A{i}:I{i}", {
                "backgroundColor": color
            })
    except Exception:
        pass  # cosmetic — never fail the sync


def _get_or_create_sheet(ss, title: str):
    """Get worksheet by title or create it."""
    try:
        return ss.worksheet(title)
    except Exception:
        return ss.add_worksheet(title=title, rows=5000, cols=20)
