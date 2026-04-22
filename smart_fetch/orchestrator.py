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
    elif name == "firecrawl":
        from smart_fetch.fetchers import firecrawl_fetcher
        return firecrawl_fetcher
    elif name in ("browserbase", "browserbase_extract"):
        from smart_fetch.fetchers import browserbase_fetcher
        return browserbase_fetcher
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


_ZILLOW_SLUG_RE = re.compile(
    r"zillow\.com/homedetails/([^/]+)/(\d+)_zpid", re.IGNORECASE
)
_STATE_CODES = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA","KS","KY","LA",
    "ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK",
    "OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY","DC",
}
_NEXT_DATA_RE = re.compile(
    r'<script[^>]+id="__NEXT_DATA__"[^>]*>(.*?)</script>', re.DOTALL
)


def _parse_slug(slug: str) -> dict:
    """Parse '10254-Shadow-Branch-Dr-Tampa-FL-33647' → address/city/state/zip."""
    parts = slug.split("-")
    zipcode = ""
    state = ""
    city_parts = []
    street_parts = []

    # Pop zip off the end if it looks like one
    if parts and parts[-1].isdigit() and len(parts[-1]) == 5:
        zipcode = parts.pop()
    # Pop state off the end if it's a 2-letter code
    if parts and parts[-1].upper() in _STATE_CODES:
        state = parts.pop().upper()
    # Walk backwards from the remainder collecting city words until we hit a
    # street suffix (Dr/St/Rd/Ave/Blvd/...) or a number (house #).
    street_suffixes = {
        "St","Ave","Rd","Dr","Ln","Blvd","Ct","Way","Pl","Pkwy","Ter","Cir",
        "Hwy","Trl","Row","Pt","Sq","Xing","Run","Loop","Aly",
    }
    i = len(parts) - 1
    while i >= 0:
        token = parts[i]
        if token in street_suffixes or (i > 0 and parts[i].isdigit()):
            # Everything up to and including this token is street
            street_parts = parts[: i + 1]
            city_parts = parts[i + 1 :]
            break
        i -= 1
    if not street_parts:
        # Fallback: assume last 1-3 tokens are the city
        split = max(1, len(parts) - 2)
        street_parts = parts[:split]
        city_parts = parts[split:]

    return {
        "address": " ".join(street_parts).strip(),
        "city": " ".join(city_parts).strip(),
        "state": state,
        "zipcode": zipcode,
    }


_BD_TO_STANDARD = {
    # BrightData key → validator/enricher key
    "price": "listing_price",
    "rentZestimate": "rent_zestimate",
    "bedrooms": "beds",
    "bathrooms": "baths",
    "livingArea": "sqft",
    "taxAssessedValue": "bd_tax_assessed",
    "lastSoldPrice": "last_sold_price",
    "yearBuilt": "year_built",
}


def _normalize_brightdata_pdp(prop: dict) -> dict:
    """Flatten BrightData PDP response into validator/enricher-compatible shape.

    BD returns `address` as a nested object and uses camelCase keys; the rest of the
    pipeline expects flat `address`/`city`/`state`/`zipcode` strings plus validator
    aliases like `listing_price`.
    """
    if not isinstance(prop, dict):
        return {}

    # Flatten address dict
    addr = prop.get("address")
    if isinstance(addr, dict):
        prop["streetAddress"] = prop.get("streetAddress") or addr.get("streetAddress")
        prop["city"] = prop.get("city") or addr.get("city")
        prop["state"] = prop.get("state") or addr.get("state")
        prop["zipcode"] = prop.get("zipcode") or addr.get("zipcode")
        prop["address"] = prop["streetAddress"] or ""
    elif isinstance(addr, str):
        prop["streetAddress"] = prop.get("streetAddress") or addr

    # Alias BD-native keys into the standard names the validator + enrichers look for.
    for bd_key, std_key in _BD_TO_STANDARD.items():
        if prop.get(bd_key) is not None and prop.get(std_key) is None:
            prop[std_key] = prop[bd_key]

    return prop


def _parse_zillow_pdp(url: str, html: str, markdown: str) -> dict:
    """Extract property fields from a Zillow PDP. URL slug is the reliable baseline;
    __NEXT_DATA__ JSON adds price/beds/baths/zestimate when present."""
    prop: dict = {}

    m = _ZILLOW_SLUG_RE.search(url)
    if m:
        slug, zpid = m.group(1), m.group(2)
        prop.update(_parse_slug(slug))
        prop["zpid"] = zpid

    if markdown:
        # Zillow PDPs front-load price/beds/baths/sqft in the hero block.
        # Restricting the search to the header avoids matches from "similar
        # homes" and "recent sales" sections further down the page.
        header = markdown[:3500]
        price_m = re.search(r"\$\s?([\d,]{4,})(?!\s*/(?:mo|sqft))", header)
        if price_m:
            try:
                prop.setdefault("price", int(price_m.group(1).replace(",", "")))
            except ValueError:
                pass
        beds_m = re.search(r"(\d+(?:\.\d+)?)[ \t]*(?:bds?|beds?|bedrooms?)\b", header, re.IGNORECASE)
        if beds_m:
            prop.setdefault("bedrooms", float(beds_m.group(1)) if "." in beds_m.group(1) else int(beds_m.group(1)))
        baths_m = re.search(r"(\d+(?:\.\d+)?)[ \t]*(?:ba(?:ths?)?|bathrooms?)\b", header, re.IGNORECASE)
        if baths_m:
            prop.setdefault("bathrooms", float(baths_m.group(1)) if "." in baths_m.group(1) else int(baths_m.group(1)))
        sqft_m = re.search(r"([\d,]{3,})[ \t]*sq\s?ft\b", header, re.IGNORECASE)
        if sqft_m:
            try:
                prop.setdefault("livingArea", int(sqft_m.group(1).replace(",", "")))
            except ValueError:
                pass
        year_m = re.search(r"Built in (\d{4})", markdown)
        if year_m:
            prop.setdefault("yearBuilt", int(year_m.group(1)))
        zest_m = re.search(r"Zestimate[^$]{0,40}\$\s?([\d,]{4,})", header, re.IGNORECASE)
        if zest_m:
            try:
                prop.setdefault("zestimate", int(zest_m.group(1).replace(",", "")))
            except ValueError:
                pass

    if html:
        nd = _NEXT_DATA_RE.search(html)
        if nd:
            try:
                blob = json.loads(nd.group(1))
                # Zillow stacks PDP data under pageProps.componentProps or gdpClientCache
                pp = (blob.get("props") or {}).get("pageProps") or {}
                cp = pp.get("componentProps") or {}
                gdp_cache_str = cp.get("gdpClientCache") or pp.get("gdpClientCache")
                if isinstance(gdp_cache_str, str):
                    try:
                        gdp = json.loads(gdp_cache_str)
                        # gdp is keyed by a ZillowQuery hash; take the first value's property
                        for v in gdp.values():
                            p = (v or {}).get("property") or {}
                            if p:
                                for k in ("price", "zestimate", "rentZestimate", "bedrooms",
                                          "bathrooms", "livingArea", "yearBuilt", "homeStatus",
                                          "homeType", "latitude", "longitude"):
                                    if p.get(k) is not None:
                                        prop.setdefault(k, p[k])
                                if p.get("address"):
                                    a = p["address"]
                                    prop.setdefault("streetAddress", a.get("streetAddress"))
                                    prop.setdefault("city", a.get("city") or prop.get("city"))
                                    prop.setdefault("state", a.get("state") or prop.get("state"))
                                    prop.setdefault("zipcode", a.get("zipcode") or prop.get("zipcode"))
                                break
                    except Exception:
                        pass
            except Exception:
                pass

    return prop


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
            elif result.get("html") or result.get("markdown"):
                # HTML/markdown fetchers (firecrawl, browserbase, playwright) — parse PDP data
                if "zillow.com/homedetails/" in url:
                    prop = _parse_zillow_pdp(url, result.get("html") or "", result.get("markdown") or "")

            fetcher_used = result.get("fetcher_used")
            # BrightData's PDP response nests the address and uses BD-native field names.
            # Flatten into the (address, city, state, zipcode) strings + validator-expected
            # aliases (listing_price, rent_zestimate, ...) so enrichers and the validator
            # see Zillow as a distinct source instead of dropping it.
            if fetcher_used == "brightdata_zillow":
                prop = _normalize_brightdata_pdp(prop)

            prop["zillow_url"] = prop.get("zillow_url") or url
            prop["_fetch_source"] = fetcher_used
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

    # Build Zillow search URLs. Zillow throttles the `homes/for_sale/{slug}_rb/`
    # path for some markets (e.g. Chicago, Detroit, Houston, Jackson, Little Rock,
    # Macon) down to ~9 results even when the market is fine. Fall back to the
    # unfiltered `homes/{slug}_rb/` path, which serves the full ~40-listing page.
    slug = f"{city.replace(' ', '-')}-{state}"
    base_urls = [
        f"https://www.zillow.com/homes/for_sale/{slug}_rb/",
        f"https://www.zillow.com/homes/{slug}_rb/",
    ]

    import re
    from concurrent.futures import ThreadPoolExecutor, as_completed

    def _collect_urls_from_page(search_url: str) -> set:
        r = fetch_url(search_url, wait_selector="article")
        if r.get("success") and r.get("html"):
            return set(re.findall(r'https://www\.zillow\.com/homedetails/[^\s"\']+', r["html"]))
        return set()

    # First try the filtered path; if the result is thin, try the unfiltered one.
    urls: set = set()
    base_used = None
    for base in base_urls:
        page_urls = _collect_urls_from_page(base)
        if len(page_urls) >= 15:
            urls = page_urls
            base_used = base
            break
        urls |= page_urls
        base_used = base  # remember the last base, even if thin

    # Pagination: Zillow serves additional listings on `{base}N_p/` (pages 2–3).
    # Widening here roughly doubles the per-market pool for free, since we
    # already pay per-market fetch latency for page 1. Pages are fetched in
    # parallel to hide their latency behind each other.
    max_pages = 3
    if base_used and len(urls) < limit * 2:
        pagination_urls = [f"{base_used}{p}_p/" for p in range(2, max_pages + 1)]
        with ThreadPoolExecutor(max_workers=len(pagination_urls)) as ex:
            for fut in as_completed(ex.submit(_collect_urls_from_page, u) for u in pagination_urls):
                urls |= fut.result()

    properties: list = []

    if urls:
        # Per-PDP fetches in parallel — each PDP may hit BrightData (MCP with
        # its own rate-limit sleep) and optionally HouseCanary + BatchData,
        # so bounded concurrency keeps bursts within downstream per-minute
        # limits while cutting total wall time ~3–4x on a 20-listing market.
        capped = list(urls)[:limit]

        def _fetch_one(url):
            return fetch_property(url=url, enrich=enrich, validate=validate)

        with ThreadPoolExecutor(max_workers=4) as ex:
            for prop in ex.map(_fetch_one, capped):
                if prop.get("address") or prop.get("streetAddress"):
                    properties.append(prop)

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
