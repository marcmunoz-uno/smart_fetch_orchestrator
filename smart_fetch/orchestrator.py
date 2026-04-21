"""Smart Fetch Orchestrator — unified property scraping with tiered fallback."""

import re
import json
import time
from typing import List, Dict, Optional
from urllib.parse import urlparse
from datetime import datetime

from smart_fetch.config import ROUTE_RULES
from smart_fetch.validator import validate_property as _validate

# Lazy imports for fetchers (they may have optional dependencies)
def _get_fetcher(name):
    if name == "curl_cffi":
        from smart_fetch.fetchers import curl_cffi_fetcher
        return curl_cffi_fetcher
    elif name in ("brightdata_zillow", "brightdata_scrape", "brightdata"):
        from smart_fetch.fetchers import brightdata_fetcher
        return brightdata_fetcher
    elif name == "playwright":
        from smart_fetch.fetchers import playwright_fetcher
        return playwright_fetcher
    elif name == "cloudflare_crawl":
        from smart_fetch.fetchers import cloudflare_fetcher
        return cloudflare_fetcher
    elif name == "requests":
        from smart_fetch.fetchers import requests_fetcher
        return requests_fetcher
    elif name == "brightdata_proxy_playwright":
        from smart_fetch.fetchers import playwright_fetcher
        return playwright_fetcher  # caller passes use_proxy=True
    return None

def _get_route(url):
    """Determine which fetcher chain to use based on URL domain."""
    parsed = urlparse(url)
    domain = parsed.netloc.lower().replace("www.", "")

    # Check if it's a Zillow PDP vs search
    if "zillow.com" in domain:
        if "/homedetails/" in url:
            return ROUTE_RULES.get("zillow.com", {}).get("pdp", ROUTE_RULES["_default"])
        return ROUTE_RULES.get("zillow.com", {}).get("search", ROUTE_RULES["_default"])

    # Check known domains
    for pattern, chain in ROUTE_RULES.items():
        if pattern.startswith("_"):
            continue
        if pattern in domain:
            return chain if isinstance(chain, list) else chain

    # Government/county heuristic
    if ".gov" in domain or "county" in domain or "assessor" in domain:
        return ROUTE_RULES["_gov"]

    return ROUTE_RULES["_default"]


def fetch_url(url: str, **kwargs) -> dict:
    """
    Fetch a URL using the appropriate fetcher chain with automatic fallback.

    Returns: {"success": bool, "html": str, "json": dict, "properties": list,
              "source": str, "error": str, "attempts": list}
    """
    chain = _get_route(url)
    attempts = []

    for fetcher_name in chain:
        fetcher = _get_fetcher(fetcher_name)
        if not fetcher:
            attempts.append({"fetcher": fetcher_name, "error": "not available"})
            continue

        extra_kwargs = dict(kwargs)
        if fetcher_name == "brightdata_proxy_playwright":
            extra_kwargs["use_proxy"] = True

        result = fetcher.fetch(url, **extra_kwargs)
        attempts.append({"fetcher": fetcher_name, "success": result.get("success"), "error": result.get("error")})

        if result.get("success"):
            result["attempts"] = attempts
            result["fetcher_used"] = fetcher_name
            return result

    return {"success": False, "error": "all fetchers failed", "attempts": attempts}


def fetch_property(url: str = None, address: str = None, city: str = None,
                   state: str = None, zipcode: str = None,
                   enrich: bool = True, validate: bool = True, **kwargs) -> dict:
    """
    Fetch and optionally enrich+validate a single property.

    Provide either:
    - url: a Zillow/listing URL
    - address + city + state + zipcode: for enrichment without scraping

    Returns property dict with all available data + validation results.
    """
    prop = {}

    # If URL provided, fetch listing data
    if url:
        result = fetch_url(url, **kwargs)

        if result.get("success"):
            # If BrightData returned structured Zillow data
            if result.get("properties"):
                prop = result["properties"][0]
            elif result.get("json"):
                prop = result["json"] if isinstance(result["json"], dict) else {}

            prop["_fetch_source"] = result.get("fetcher_used")
            prop["_fetch_attempts"] = result.get("attempts")
        else:
            prop["_fetch_error"] = result.get("error")
            prop["_fetch_attempts"] = result.get("attempts")

    # Set address fields if provided directly
    if address: prop.setdefault("address", address)
    if city: prop.setdefault("city", city)
    if state: prop.setdefault("state", state)
    if zipcode: prop.setdefault("zipcode", zipcode)

    # Enrich with multiple sources
    if enrich and (prop.get("address") or prop.get("streetAddress")):
        # Normalize address fields for enrichers
        if prop.get("streetAddress") and not prop.get("address"):
            prop["address"] = prop["streetAddress"]
        if not prop.get("zipcode") and prop.get("zip"):
            prop["zipcode"] = prop["zip"]

        # HouseCanary enrichment
        try:
            from smart_fetch.enrichers.housecanary_enricher import enrich as hc_enrich
            prop = hc_enrich(prop)
        except Exception as e:
            prop["_hc_error"] = str(e)

        # BrightData enrichment (if we have a Zillow URL and haven't already fetched via BD)
        if prop.get("zillow_url") and prop.get("_fetch_source") != "brightdata_zillow":
            try:
                from smart_fetch.enrichers.brightdata_enricher import enrich as bd_enrich
                prop = bd_enrich(prop)
            except Exception as e:
                prop["_bd_error"] = str(e)

    # Validate
    if validate:
        prop["_validation"] = _validate(prop)

    prop["_fetched_at"] = datetime.utcnow().isoformat() + "Z"
    return prop


def fetch_market(city: str, state: str, min_price: int = 50000, max_price: int = 300000,
                 limit: int = 20, enrich: bool = True, validate: bool = True) -> List[dict]:
    """
    Fetch for-sale properties in a market.

    Tries Zillow search via curl_cffi first, then falls back to
    BrightData enrichment of cached data.

    Returns list of property dicts.
    """
    from pathlib import Path

    # Build Zillow search URL
    slug = f"{city.replace(' ', '-')}-{state}"
    search_url = f"https://www.zillow.com/homes/for_sale/{slug}_rb/"

    # Try fetching the search page
    result = fetch_url(search_url, wait_selector="article")

    properties = []

    if result.get("success") and result.get("html"):
        # Parse listing URLs from HTML
        import re
        urls = set(re.findall(r'https://www\.zillow\.com/homedetails/[^\s"\']+', result["html"]))

        for url in list(urls)[:limit]:
            prop = fetch_property(url=url, enrich=enrich, validate=validate)
            if prop.get("address") or prop.get("streetAddress"):
                properties.append(prop)
            time.sleep(1)

    if not properties:
        # Fallback: load cached data and enrich
        cache_file = Path("/tmp/for_sale_properties.json")
        if cache_file.exists():
            try:
                cached = json.loads(cache_file.read_text())
                city_lower = city.lower()
                relevant = [p for p in cached if (p.get("city") or "").lower() == city_lower][:limit]

                for prop in relevant:
                    if enrich:
                        try:
                            from smart_fetch.enrichers.housecanary_enricher import enrich as hc_enrich
                            prop = hc_enrich(prop)
                        except:
                            pass
                        try:
                            from smart_fetch.enrichers.brightdata_enricher import enrich as bd_enrich
                            prop = bd_enrich(prop)
                        except:
                            pass
                    if validate:
                        prop["_validation"] = _validate(prop)
                    properties.append(prop)
            except:
                pass

    return properties


def validate_property(prop: dict) -> dict:
    """Public API: validate a property dict with three-source cross-validation."""
    return _validate(prop)
