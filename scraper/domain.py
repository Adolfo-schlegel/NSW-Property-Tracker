"""
scraper/domain.py — Domain.com.au scraper using Playwright
"""
import logging
import re
import time
from typing import Optional

import config
from scraper.base import BaseScraper, parse_price, extract_suburb_postcode

logger = logging.getLogger(__name__)

DOMAIN_BASE = "https://www.domain.com.au"

SEARCH_URLS = {
    "sale": f"{DOMAIN_BASE}/sale/sydney-nsw/",
    "rent": f"{DOMAIN_BASE}/rent/sydney-nsw/",
}


class DomainScraper(BaseScraper):
    source = "domain"

    def scrape(self, listing_type: str = "sale", max_pages: int = 50) -> list[dict]:
        self.listings = []
        base_url = SEARCH_URLS.get(listing_type, SEARCH_URLS["sale"])

        for page_num in range(1, max_pages + 1):
            url = base_url if page_num == 1 else f"{base_url}?page={page_num}"
            logger.info("[domain] scraping page %d: %s", page_num, url)

            listings_on_page = self._scrape_page(url, listing_type)

            if not listings_on_page:
                logger.info("[domain] no listings on page %d — stopping", page_num)
                break

            self.listings.extend(listings_on_page)
            logger.info(
                "[domain] page %d: %d listings (total: %d)",
                page_num, len(listings_on_page), len(self.listings)
            )

            # Check if there's a next page
            if not self._has_next_page():
                logger.info("[domain] last page reached at %d", page_num)
                break

            # Polite delay between pages
            time.sleep(config.SCRAPE_DELAY_MS / 1000)

        logger.info("[domain] scrape complete: %d total listings", len(self.listings))
        return self.listings

    def _scrape_page(self, url: str, listing_type: str) -> list[dict]:
        for attempt in range(config.RETRY_ATTEMPTS):
            try:
                self.page.goto(url, timeout=config.SCRAPE_TIMEOUT_MS, wait_until="domcontentloaded")
                self.page.wait_for_timeout(2000)

                # Wait for listing cards to appear
                self.page.wait_for_selector(
                    "[data-testid='listing-card-wrapper-premiums'], "
                    "[data-testid='listing-card-wrapper-standard'], "
                    "article.css-1qp9106",
                    timeout=15000
                )
                break
            except Exception as e:
                logger.warning("[domain] page load attempt %d failed: %s", attempt + 1, e)
                if attempt == config.RETRY_ATTEMPTS - 1:
                    return []
                time.sleep(3)

        results = []

        # Try multiple selector strategies (Domain changes their markup periodically)
        selectors = [
            "[data-testid='listing-card-wrapper-premiums']",
            "[data-testid='listing-card-wrapper-standard']",
            "article[data-testid]",
            "article.css-1qp9106",
            "[data-testid='listing-card']",
        ]

        elements = []
        for selector in selectors:
            try:
                elements = self.page.query_selector_all(selector)
                if elements:
                    logger.debug("[domain] found %d elements with: %s", len(elements), selector)
                    break
            except Exception:
                continue

        if not elements:
            # Fallback: try to get listing URLs from links
            return self._fallback_link_scrape(listing_type)

        for el in elements:
            listing = self._parse_listing_element(el, listing_type)
            if listing:
                results.append(listing)

        return results

    def _parse_listing_element(self, el, listing_type: str) -> Optional[dict]:
        try:
            # Extract URL and ID
            link = el.query_selector("a[href*='/']")
            if not link:
                return None

            href = link.get_attribute("href") or ""
            if not href.startswith("http"):
                href = DOMAIN_BASE + href

            # ID is the last numeric segment in the URL
            id_match = re.search(r"-(\d{7,12})$", href.rstrip("/"))
            if not id_match:
                return None
            listing_id = f"domain_{id_match.group(1)}"

            # Address
            address = ""
            for addr_sel in [
                "[data-testid='address']",
                "[data-testid='listing-card-address']",
                "h2",
                "[class*='address']",
            ]:
                addr_el = el.query_selector(addr_sel)
                if addr_el:
                    address = addr_el.inner_text().strip()
                    break

            # Price
            price = ""
            for price_sel in [
                "[data-testid='listing-card-price']",
                "[class*='price']",
                "[data-testid='price']",
            ]:
                price_el = el.query_selector(price_sel)
                if price_el:
                    price = price_el.inner_text().strip()
                    break

            # Bedrooms / Bathrooms / Parking
            bedrooms = bathrooms = car_spaces = None
            for feat_sel in [
                "[data-testid='listing-card-features']",
                "[class*='features']",
                "[class*='PropertyFeatures']",
            ]:
                feat_el = el.query_selector(feat_sel)
                if feat_el:
                    feat_text = feat_el.inner_text()
                    b = re.search(r"(\d+)\s*(?:bed|Bed|room|Room|\u6E... )", feat_text)
                    ba = re.search(r"(\d+)\s*(?:bath|Bath)", feat_text)
                    c = re.search(r"(\d+)\s*(?:car|Car|park|Park)", feat_text)
                    if b:  bedrooms  = int(b.group(1))
                    if ba: bathrooms = int(ba.group(1))
                    if c:  car_spaces = int(c.group(1))
                    break

            # Try feature icons (span with aria-labels)
            if bedrooms is None:
                spans = el.query_selector_all("span[aria-label]")
                for span in spans:
                    label = (span.get_attribute("aria-label") or "").lower()
                    val_el = span.query_selector("span") or span
                    try:
                        val = int(val_el.inner_text().strip())
                        if "bed"  in label: bedrooms   = val
                        if "bath" in label: bathrooms  = val
                        if "car"  in label or "park" in label: car_spaces = val
                    except (ValueError, Exception):
                        pass

            # Agent / Agency
            agent = agency = ""
            for ag_sel in ["[data-testid='agent-name']", "[class*='agent']"]:
                ag_el = el.query_selector(ag_sel)
                if ag_el:
                    agent = ag_el.inner_text().strip()
                    break

            suburb, postcode = extract_suburb_postcode(address)
            if config.SUBURBS and suburb and suburb.lower() not in [s.lower() for s in config.SUBURBS]:
                return None

            price_value = parse_price(price)

            return {
                "id":            listing_id,
                "source":        "domain",
                "url":           href,
                "address":       address,
                "suburb":        suburb,
                "postcode":      postcode,
                "price":         price,
                "price_value":   price_value,
                "bedrooms":      bedrooms,
                "bathrooms":     bathrooms,
                "car_spaces":    car_spaces,
                "property_type": self._detect_type(address, el),
                "agent":         agent,
                "agency":        agency,
                "listing_type":  listing_type,
            }

        except Exception as e:
            logger.debug("[domain] parse error: %s", e)
            return None

    def _detect_type(self, address: str, el) -> str:
        text = (address + " " + (el.inner_text() if el else "")).lower()
        if "apartment" in text or "unit" in text or "apt" in text:
            return "apartment"
        if "townhouse" in text or "town house" in text:
            return "townhouse"
        if "studio" in text:
            return "studio"
        if "land" in text or "block" in text:
            return "land"
        return "house"

    def _fallback_link_scrape(self, listing_type: str) -> list[dict]:
        """Minimal fallback: extract IDs from listing links on the page."""
        links = self.page.query_selector_all("a[href*='domain.com.au']")
        results = []
        for link in links:
            href = link.get_attribute("href") or ""
            m = re.search(r"-(\d{7,12})$", href.rstrip("/"))
            if m:
                lid = f"domain_{m.group(1)}"
                results.append({
                    "id": lid, "source": "domain",
                    "url": href if href.startswith("http") else DOMAIN_BASE + href,
                    "address": "", "suburb": "", "postcode": "",
                    "price": "", "price_value": None,
                    "bedrooms": None, "bathrooms": None, "car_spaces": None,
                    "property_type": "unknown", "agent": "", "agency": "",
                    "listing_type": listing_type,
                })
        return results

    def _has_next_page(self) -> bool:
        try:
            next_btn = self.page.query_selector(
                "[data-testid='paginator-navigation-button-next']:not([disabled]), "
                "a[aria-label='Next page']:not([disabled]), "
                "a[rel='next']"
            )
            return next_btn is not None
        except Exception:
            return False

    def _parse_listing(self, element) -> Optional[dict]:
        """Required by ABC — delegates to _parse_listing_element."""
        return self._parse_listing_element(element, config.LISTING_TYPE)
