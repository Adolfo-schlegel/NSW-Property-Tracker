"""
scraper/realestate.py — REA Group (realestate.com.au) scraper
"""
import logging
import re
import time
from typing import Optional

import config
from scraper.base import BaseScraper, parse_price, extract_suburb_postcode

logger = logging.getLogger(__name__)

REA_BASE = "https://www.realestate.com.au"

SEARCH_URLS = {
    "sale": f"{REA_BASE}/buy/in-sydney%2C+nsw/list-1",
    "rent": f"{REA_BASE}/rent/in-sydney%2C+nsw/list-1",
}


class RealEstateScraper(BaseScraper):
    source = "realestate"

    def scrape(self, listing_type: str = "sale", max_pages: int = 50) -> list[dict]:
        self.listings = []
        base_url = SEARCH_URLS.get(listing_type, SEARCH_URLS["sale"])

        for page_num in range(1, max_pages + 1):
            # REA uses /list-N suffix for pagination
            url = re.sub(r"/list-\d+", f"/list-{page_num}", base_url)
            logger.info("[realestate] scraping page %d: %s", page_num, url)

            listings_on_page = self._scrape_page(url, listing_type)

            if not listings_on_page:
                logger.info("[realestate] no listings on page %d — stopping", page_num)
                break

            self.listings.extend(listings_on_page)
            logger.info(
                "[realestate] page %d: %d listings (total: %d)",
                page_num, len(listings_on_page), len(self.listings)
            )

            if not self._has_next_page():
                break

            time.sleep(config.SCRAPE_DELAY_MS / 1000)

        logger.info("[realestate] scrape complete: %d total listings", len(self.listings))
        return self.listings

    def _scrape_page(self, url: str, listing_type: str) -> list[dict]:
        for attempt in range(config.RETRY_ATTEMPTS):
            try:
                self.page.goto(url, timeout=config.SCRAPE_TIMEOUT_MS, wait_until="domcontentloaded")
                self.page.wait_for_timeout(2500)
                self.page.wait_for_selector(
                    "[data-testid='results-list'], "
                    "[class*='residential-card'], "
                    "article[class*='Card']",
                    timeout=15000
                )
                break
            except Exception as e:
                logger.warning("[realestate] load attempt %d failed: %s", attempt + 1, e)
                if attempt == config.RETRY_ATTEMPTS - 1:
                    return []
                time.sleep(3)

        results = []
        selectors = [
            "[data-testid='results-list'] > div > div",
            "[class*='residential-card__content']",
            "article[data-testid='listing-card']",
            "[class*='ResidentialCard']",
        ]

        elements = []
        for sel in selectors:
            try:
                elements = self.page.query_selector_all(sel)
                if elements:
                    break
            except Exception:
                continue

        if not elements:
            return self._fallback_link_scrape(listing_type)

        for el in elements:
            listing = self._parse_listing_element(el, listing_type)
            if listing:
                results.append(listing)

        return results

    def _parse_listing_element(self, el, listing_type: str) -> Optional[dict]:
        try:
            # Find the anchor with listing URL
            link = el.query_selector("a[href*='/property-']") or el.query_selector("a[href]")
            if not link:
                return None

            href = link.get_attribute("href") or ""
            if not href.startswith("http"):
                href = REA_BASE + href

            # REA listing IDs are numeric, in URL like /property-house-nsw-sydney-12345678
            id_match = re.search(r"-(\d{7,12})$", href.rstrip("/"))
            if not id_match:
                # Try query param
                id_match = re.search(r"id=(\d+)", href)
                if not id_match:
                    return None
            listing_id = f"rea_{id_match.group(1)}"

            # Address
            address = ""
            for sel in [
                "[data-testid='address']",
                "[class*='residential-card__address']",
                "h2", "h3", "[class*='Address']",
            ]:
                addr_el = el.query_selector(sel)
                if addr_el:
                    address = addr_el.inner_text().strip()
                    if address:
                        break

            # Price
            price = ""
            for sel in [
                "[data-testid='listing-card-price']",
                "[class*='residential-card__price']",
                "[class*='Price']", "[data-testid='price']",
            ]:
                price_el = el.query_selector(sel)
                if price_el:
                    price = price_el.inner_text().strip()
                    if price:
                        break

            # Features
            bedrooms = bathrooms = car_spaces = None
            feat_text = el.inner_text()
            b  = re.search(r"(\d+)\s*(?:Bed|bed)", feat_text)
            ba = re.search(r"(\d+)\s*(?:Bath|bath)", feat_text)
            c  = re.search(r"(\d+)\s*(?:Car|car|Park|park)", feat_text)
            if b:  bedrooms   = int(b.group(1))
            if ba: bathrooms  = int(ba.group(1))
            if c:  car_spaces = int(c.group(1))

            suburb, postcode = extract_suburb_postcode(address)
            if config.SUBURBS and suburb and suburb.lower() not in [s.lower() for s in config.SUBURBS]:
                return None

            # Property type from URL
            prop_type = "house"
            url_lower = href.lower()
            if "apartment" in url_lower or "unit" in url_lower:
                prop_type = "apartment"
            elif "townhouse" in url_lower:
                prop_type = "townhouse"
            elif "land" in url_lower or "block" in url_lower:
                prop_type = "land"

            return {
                "id":            listing_id,
                "source":        "realestate",
                "url":           href,
                "address":       address,
                "suburb":        suburb,
                "postcode":      postcode,
                "price":         price,
                "price_value":   parse_price(price),
                "bedrooms":      bedrooms,
                "bathrooms":     bathrooms,
                "car_spaces":    car_spaces,
                "property_type": prop_type,
                "agent":         "",
                "agency":        "",
                "listing_type":  listing_type,
            }

        except Exception as e:
            logger.debug("[realestate] parse error: %s", e)
            return None

    def _fallback_link_scrape(self, listing_type: str) -> list[dict]:
        links = self.page.query_selector_all("a[href*='/property-']")
        results = []
        seen = set()
        for link in links:
            href = link.get_attribute("href") or ""
            m = re.search(r"-(\d{7,12})$", href.rstrip("/"))
            if m and m.group(1) not in seen:
                seen.add(m.group(1))
                results.append({
                    "id": f"rea_{m.group(1)}", "source": "realestate",
                    "url": href if href.startswith("http") else REA_BASE + href,
                    "address": "", "suburb": "", "postcode": "",
                    "price": "", "price_value": None,
                    "bedrooms": None, "bathrooms": None, "car_spaces": None,
                    "property_type": "unknown", "agent": "", "agency": "",
                    "listing_type": listing_type,
                })
        return results

    def _has_next_page(self) -> bool:
        try:
            btn = self.page.query_selector(
                "[data-testid='paginator-navigation-button-next']:not([disabled]), "
                "a[aria-label='Next page']:not([disabled])"
            )
            return btn is not None
        except Exception:
            return False

    def _parse_listing(self, element) -> Optional[dict]:
        return self._parse_listing_element(element, config.LISTING_TYPE)
