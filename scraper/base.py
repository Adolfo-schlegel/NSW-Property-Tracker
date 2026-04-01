"""
scraper/base.py — Shared Playwright utilities and base scraper class
"""
import logging
import re
import time
from abc import ABC, abstractmethod
from typing import Optional

logger = logging.getLogger(__name__)


def parse_price(raw: str) -> Optional[int]:
    """
    Convert price strings to integer values.
    Examples:
      '$1,200,000'  → 1200000
      '$850k'       → 850000
      '$1.2m'       → 1200000
      '$800 pw'     → 800         (per week — rent)
      'Contact agent' → None
    """
    if not raw:
        return None
    s = raw.lower().replace(",", "").replace("$", "").replace(" ", "")
    # Per week / per month — return weekly price
    pw = re.search(r"(\d+\.?\d*)\s*pw", s)
    pm = re.search(r"(\d+\.?\d*)\s*pm", s)
    if pw:
        return int(float(pw.group(1)))
    if pm:
        return int(float(pm.group(1)) / 4.33)

    # Millions
    m = re.search(r"(\d+\.?\d*)m", s)
    if m:
        return int(float(m.group(1)) * 1_000_000)
    # Thousands
    k = re.search(r"(\d+\.?\d*)k", s)
    if k:
        return int(float(k.group(1)) * 1_000)
    # Plain number
    n = re.search(r"(\d+)", s)
    if n:
        val = int(n.group(1))
        # Sanity check — raw number below 10k is probably weekly rent
        return val
    return None


def extract_suburb_postcode(address: str) -> tuple[str, str]:
    """
    Extract suburb and postcode from an address string.
    '12 Smith St, Surry Hills NSW 2010' → ('Surry Hills', '2010')
    """
    if not address:
        return ("", "")
    # Postcode
    pc_match = re.search(r"\b(\d{4})\b", address)
    postcode  = pc_match.group(1) if pc_match else ""
    # Suburb (word(s) before NSW/VIC/QLD and postcode)
    sub_match = re.search(r",\s*([^,]+?)\s+(?:NSW|VIC|QLD|SA|WA|ACT|TAS|NT)\b", address, re.IGNORECASE)
    suburb    = sub_match.group(1).strip().title() if sub_match else ""
    return suburb, postcode


class BaseScraper(ABC):
    """Abstract base class for property scrapers."""

    source: str = "unknown"

    def __init__(self, playwright, headless: bool = True):
        self.pw        = playwright
        self.headless  = headless
        self.browser   = None
        self.context   = None
        self.page      = None
        self.listings: list[dict] = []

    def start(self):
        self.browser = self.pw.chromium.launch(
            headless=self.headless,
            args=[
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
            ],
        )
        self.context = self.browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 800},
            locale="en-AU",
        )
        # Block images/fonts to speed up scraping
        self.context.route(
            "**/*.{png,jpg,jpeg,gif,webp,svg,woff,woff2,ttf,eot}",
            lambda route: route.abort()
        )
        self.page = self.context.new_page()
        logger.info("[%s] browser started (headless=%s)", self.source, self.headless)

    def stop(self):
        if self.browser:
            self.browser.close()
            logger.info("[%s] browser closed", self.source)

    def wait(self, ms: int):
        time.sleep(ms / 1000)

    @abstractmethod
    def scrape(self, listing_type: str = "sale", max_pages: int = 50) -> list[dict]:
        """Run the full scrape. Returns list of listing dicts."""
        ...

    @abstractmethod
    def _parse_listing(self, element) -> Optional[dict]:
        """Extract data from a single listing element."""
        ...
