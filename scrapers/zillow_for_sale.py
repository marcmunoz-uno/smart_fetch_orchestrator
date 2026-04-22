#!/usr/bin/env python3
"""
Zillow For-Sale Monitor - Find cash-flowing investment properties for sale.

Searches Zillow for-sale listings for target markets and calculates DSCR numbers.
"""

import argparse
import json
import os
import random
import re
import sys
import time
import math
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from curl_cffi import requests as cf
from loguru import logger as log

try:
    from smart_fetch.orchestrator import fetch_url as sf_fetch
    from smart_fetch.orchestrator import fetch_market as sf_fetch_market
    SMART_FETCH_AVAILABLE = True
except ImportError:
    SMART_FETCH_AVAILABLE = False

# Configure logging
log.remove()
log.add(sys.stderr, level="INFO", format="<green>{time:HH:mm:ss}</green> | <level>{level:<7}</level> | {message}")

# --- Constants ---
DEFAULT_MARKETS = [
    "Indianapolis IN", "Cleveland OH", "Birmingham AL", "Memphis TN",
    "Detroit MI", "St Louis MO", "Milwaukee WI", "Columbus OH",
    "Jacksonville FL", "Jackson MS", "Lorain OH", "Toledo OH",
    "Akron OH", "Dayton OH", "Miami FL", "Houston TX", "Dallas TX",
    "Atlanta GA", "Phoenix AZ", "Kansas City MO",
    # Added 2026-04-16 — expansion batch
    "Tampa FL", "Orlando FL", "Pittsburgh PA", "New Orleans LA",
    "Baton Rouge LA", "Tulsa OK", "Little Rock AR", "Macon GA",
    "Oklahoma City OK", "Lansing MI", "Chicago IL",
]

PRICE_MIN = 50_000
PRICE_MAX = 300_000
MAX_DAYS_ON_MARKET = 60  # Skip properties sitting too long (likely problem properties)

# Approximate map bounds for target markets
MARKET_BOUNDS = {
    "Miami FL":         {"west": -80.35, "east": -80.12, "south": 25.70, "north": 25.87},
    "Houston TX":       {"west": -95.65, "east": -95.25, "south": 29.62, "north": 29.88},
    "Dallas TX":        {"west": -96.95, "east": -96.65, "south": 32.68, "north": 32.92},
    "Atlanta GA":       {"west": -84.55, "east": -84.28, "south": 33.65, "north": 33.88},
    "Phoenix AZ":       {"west": -112.20, "east": -111.90, "south": 33.35, "north": 33.60},
    "Indianapolis IN":  {"west": -86.33, "east": -85.94, "south": 39.63, "north": 39.93},
    "Kansas City MO":   {"west": -94.70, "east": -94.40, "south": 38.95, "north": 39.18},
    "Cleveland OH":     {"west": -81.85, "east": -81.55, "south": 41.40, "north": 41.55},
    "Birmingham AL":    {"west": -86.90, "east": -86.70, "south": 33.44, "north": 33.58},
    "Memphis TN":       {"west": -90.15, "east": -89.85, "south": 35.00, "north": 35.22},
    "Detroit MI":       {"west": -83.30, "east": -82.90, "south": 42.28, "north": 42.45},
    "St Louis MO":      {"west": -90.40, "east": -90.15, "south": 38.55, "north": 38.75},
    "Milwaukee WI":     {"west": -88.05, "east": -87.85, "south": 42.95, "north": 43.10},
    "Columbus OH":      {"west": -83.10, "east": -82.80, "south": 39.90, "north": 40.10},
    "Jacksonville FL":  {"west": -81.80, "east": -81.50, "south": 30.25, "north": 30.45},
    "Jackson MS":       {"west": -90.30, "east": -90.05, "south": 32.25, "north": 32.40},
    "Lorain OH":        {"west": -82.25, "east": -82.10, "south": 41.43, "north": 41.48},
    "Toledo OH":        {"west": -83.65, "east": -83.45, "south": 41.62, "north": 41.72},
    "Akron OH":         {"west": -81.60, "east": -81.45, "south": 41.04, "north": 41.12},
    "Dayton OH":        {"west": -84.25, "east": -84.10, "south": 39.72, "north": 39.82},
    # Added 2026-04-16 — expansion batch
    "Tampa FL":         {"west": -82.55, "east": -82.38, "south": 27.88, "north": 28.02},
    "Orlando FL":       {"west": -81.48, "east": -81.28, "south": 28.45, "north": 28.60},
    "Pittsburgh PA":    {"west": -80.05, "east": -79.88, "south": 40.38, "north": 40.50},
    "New Orleans LA":   {"west": -90.15, "east": -89.95, "south": 29.92, "north": 30.05},
    "Baton Rouge LA":   {"west": -91.22, "east": -91.08, "south": 30.38, "north": 30.52},
    "Tulsa OK":         {"west": -96.05, "east": -95.85, "south": 36.05, "north": 36.22},
    "Little Rock AR":   {"west": -92.35, "east": -92.20, "south": 34.68, "north": 34.82},
    "Macon GA":         {"west": -83.72, "east": -83.58, "south": 32.78, "north": 32.92},
    "Oklahoma City OK": {"west": -97.62, "east": -97.40, "south": 35.38, "north": 35.55},
    "Lansing MI":       {"west": -84.60, "east": -84.48, "south": 42.68, "north": 42.78},
    "Chicago IL":       {"west": -87.85, "east": -87.55, "south": 41.75, "north": 42.00},
}

BASE_HEADERS = {
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.9",
    "accept-encoding": "gzip, deflate, br",
}


def build_search_payload(market: str, bounds: dict, page: int = 1) -> dict:
    """Build search API payload for for-sale listings."""
    payload = {
        "searchQueryState": {
            "pagination": {} if page <= 1 else {"currentPage": page},
            "usersSearchTerm": market.replace(" ", ", "),  # "Indianapolis, IN"
            "mapBounds": bounds,
            "filterState": {
                "isForSaleByAgent": {"value": True},
                "isForSaleByOwner": {"value": True},
                "isForRent": {"value": False},
                "isNewConstruction": {"value": False},
                "isComingSoon": {"value": False},
                "isAuction": {"value": False},
                "isForSaleForeclosure": {"value": False},
                "isAllHomes": {"value": True},
                "price": {"min": PRICE_MIN, "max": PRICE_MAX},
                # Focus on single-family and multi-family homes
                "isApartment": {"value": False},
                "isCondo": {"value": False},
                "isManufactured": {"value": False},
            },
            "isListVisible": True,
        },
        "wants": {"cat1": ["listResults", "mapResults"], "cat2": ["total"]},
        "requestId": random.randint(2, 20),
    }
    return payload


def extract_listing(item: dict) -> Optional[dict]:
    """Extract a property from a Zillow map/list result item."""
    # Skip apartment buildings
    if item.get("isBuilding"):
        return None

    zpid = str(item.get("zpid", ""))
    if not zpid:
        return None

    hdp = item.get("hdpData", {}).get("homeInfo", {})
    home_type = hdp.get("homeType", "")

    # Filter to residential types
    if home_type not in ("SINGLE_FAMILY", "MULTI_FAMILY", "TOWNHOUSE"):
        return None

    # Listing price
    price_raw = item.get("price", "")
    if isinstance(price_raw, str):
        nums = re.findall(r"[\d,]+", price_raw.replace(",", ""))
        listing_price = int(nums[0]) if nums else 0
    else:
        listing_price = int(price_raw or 0)

    # Also check hdp price
    if not listing_price:
        listing_price = int(hdp.get("price", 0))

    if listing_price < PRICE_MIN or listing_price > PRICE_MAX:
        return None

    # Rent Zestimate - this is critical for DSCR calculation
    rent_zestimate = hdp.get("rentZestimate", 0) or 0
    if not rent_zestimate:
        return None  # Skip properties without rent estimate

    # Tax assessed value
    tax_assessed_value = hdp.get("taxAssessedValue", 0) or 0

    # Days on market from timeOnZillow (milliseconds)
    time_on = hdp.get("timeOnZillow") or item.get("timeOnZillow") or 0
    days_on_market = int(time_on / 86_400_000) if time_on > 0 else 0
    # Override with daysOnZillow if valid
    doz = hdp.get("daysOnZillow", -1)
    if doz and doz > 0:
        days_on_market = doz

    # Skip properties on market too long (likely problem properties / need heavy rehab)
    if days_on_market > MAX_DAYS_ON_MARKET and days_on_market > 0:
        return None

    address = item.get("address", "")
    street = hdp.get("streetAddress", "") or address.split(",")[0].strip()
    city = hdp.get("city", "")
    state = hdp.get("state", "")
    zipcode = hdp.get("zipcode", "")

    detail_url = item.get("detailUrl", "")
    if detail_url and not detail_url.startswith("http"):
        detail_url = f"https://www.zillow.com{detail_url}"

    # Photo URL
    photo_url = ""
    if "imgSrc" in item:
        photo_url = item["imgSrc"]
    elif "photos" in item and item["photos"]:
        photo_url = item["photos"][0].get("url", "")

    # Listing agent info — Zillow embeds this in attributionInfo or brokerName fields
    attribution = item.get("attributionInfo") or hdp.get("attributionInfo") or {}
    listing_agent_name = (
        attribution.get("agentName", "")
        or item.get("brokerName", "")
        or hdp.get("brokerName", "")
        or ""
    )
    listing_agent_email = attribution.get("agentEmail", "") or ""
    listing_agent_phone = (
        attribution.get("agentPhoneNumber", "")
        or item.get("brokerPhoneNumber", "")
        or hdp.get("brokerPhoneNumber", "")
        or ""
    )

    return {
        "zpid": zpid,
        "address": street,
        "city": city,
        "state": state,
        "zipcode": zipcode,
        "listing_price": listing_price,
        "rent_zestimate": rent_zestimate,
        "tax_assessed_value": tax_assessed_value,
        "days_on_market": days_on_market,
        "home_type": home_type,
        "beds": hdp.get("bedrooms", 0) or item.get("beds", 0),
        "baths": hdp.get("bathrooms", 0) or item.get("baths", 0),
        "sqft": hdp.get("livingArea", 0) or item.get("area", 0),
        "zillow_url": detail_url,
        "photo_url": photo_url,
        "listing_agent_name": listing_agent_name,
        "listing_agent_email": listing_agent_email,
        "listing_agent_phone": listing_agent_phone,
    }


def calculate_dscr_metrics(property_data: dict) -> dict:
    """Calculate DSCR and cash flow metrics for a property."""
    purchase_price = property_data["listing_price"]
    rent = property_data["rent_zestimate"]
    tax_value = property_data["tax_assessed_value"] or purchase_price
    
    # DSCR loan assumptions
    down_payment = purchase_price * 0.20
    loan_amount = purchase_price - down_payment
    interest_rate = 0.075  # 7.5%
    term_years = 30
    
    # Monthly P&I calculation
    monthly_rate = interest_rate / 12
    num_payments = term_years * 12
    monthly_pi = loan_amount * (monthly_rate * (1 + monthly_rate)**num_payments) / ((1 + monthly_rate)**num_payments - 1)
    
    # Monthly taxes and insurance
    monthly_taxes = (tax_value * 0.01) / 12  # 1% annual
    monthly_insurance = (purchase_price * 0.005) / 12  # 0.5% annual
    
    # Total PITI
    piti = monthly_pi + monthly_taxes + monthly_insurance
    
    # Property management (10% of rent)
    property_management = rent * 0.10
    
    # Cash flow
    monthly_cash_flow = rent - piti - property_management
    annual_cash_flow = monthly_cash_flow * 12
    
    # DSCR ratio
    dscr_ratio = rent / piti if piti > 0 else 0
    
    # Cash-on-Cash return
    cash_on_cash = (annual_cash_flow / down_payment * 100) if down_payment > 0 else 0
    
    # Update the property data with calculations
    property_data.update({
        "down_payment": down_payment,
        "loan_amount": loan_amount,
        "monthly_pi": monthly_pi,
        "monthly_taxes": monthly_taxes,
        "monthly_insurance": monthly_insurance,
        "piti": piti,
        "property_management": property_management,
        "monthly_cash_flow": monthly_cash_flow,
        "annual_cash_flow": annual_cash_flow,
        "dscr_ratio": dscr_ratio,
        "cash_on_cash": cash_on_cash,
    })
    
    return property_data


def try_smart_fetch(markets: List[str], min_coc: float, limit_per_market: int = 20) -> List[dict]:
    """Tier 1: smart_fetch search-page → PDP chain (Firecrawl bypass of PX).

    Replaces the curl_cffi Zillow internal-API path, which has been 403'd since
    Zillow tightened PX. smart_fetch's Firecrawl tier scrapes the search HTML
    and parses per-PDP data (address/price/beds/baths/sqft + optional
    enrichment). We skip enrich/validate here to keep the nightly run fast —
    downstream pipelines (zillow_to_mcp_bridge, tranchi_daily) run their own
    enrichment/validation pass.
    """
    if not SMART_FETCH_AVAILABLE:
        log.warning("smart_fetch not importable; skipping Tier 1")
        return []

    from concurrent.futures import ThreadPoolExecutor, as_completed

    def _process_market(market: str) -> tuple:
        """Scrape + qualify one market. Returns (market, qualifying_properties, scraped_count, error)."""
        city, _, state = market.rpartition(" ")
        if not city or not state:
            return (market, [], 0, f"invalid market format: {market!r}")

        try:
            # enrich=True so HouseCanary fills in rent_zestimate when Zillow's
            # own estimate is null (common for freshly-listed homes). Without
            # rent, no CoC can be computed → property is silently dropped.
            props = sf_fetch_market(city, state, limit=limit_per_market,
                                    enrich=True, validate=False)
        except Exception as e:
            return (market, [], 0, f"smart_fetch error: {e}")

        qualifying: List[dict] = []
        for p in props:
            # Map smart_fetch shape → zillow_for_sale schema
            listing_price = p.get("listing_price") or p.get("price") or 0
            # Rent priority: Zillow rentZestimate (live) → HouseCanary AVM rent
            # → no property (can't compute CoC without a rent figure).
            rent = (p.get("rent_zestimate")
                    or p.get("rentZestimate")
                    or p.get("hc_rent_mean")
                    or 0)
            if not listing_price or not rent:
                continue
            if listing_price < PRICE_MIN or listing_price > PRICE_MAX:
                continue
            home_type = p.get("homeType") or p.get("home_type") or ""
            if home_type and home_type not in ("SINGLE_FAMILY", "MULTI_FAMILY", "TOWNHOUSE"):
                continue

            bd_raw = p.get("_bd_raw") or {}
            attribution = bd_raw.get("attributionInfo") or {}
            photo_url = p.get("photo_url") or ""
            if not photo_url:
                imgs = bd_raw.get("hugePhotos") or bd_raw.get("originalPhotos") or []
                if imgs and isinstance(imgs, list):
                    first = imgs[0]
                    photo_url = first.get("url", "") if isinstance(first, dict) else str(first)

            prop = {
                "zpid": p.get("zpid") or bd_raw.get("zpid") or "",
                "address": p.get("streetAddress") or p.get("address") or "",
                "city": p.get("city") or city,
                "state": (p.get("state") or state).upper(),
                "zipcode": p.get("zipcode") or "",
                "listing_price": int(listing_price),
                "rent_zestimate": int(rent),
                "tax_assessed_value": int(p.get("bd_tax_assessed") or p.get("taxAssessedValue") or 0),
                "days_on_market": int(bd_raw.get("daysOnZillow") or bd_raw.get("days_on_zillow") or 0),
                "home_type": home_type or "SINGLE_FAMILY",
                "beds": p.get("beds") or p.get("bedrooms") or 0,
                "baths": p.get("baths") or p.get("bathrooms") or 0,
                "sqft": p.get("sqft") or p.get("livingArea") or 0,
                "zillow_url": p.get("zillow_url") or bd_raw.get("hdpUrl") or "",
                "photo_url": photo_url,
                "listing_agent_name": attribution.get("agentName", "") or bd_raw.get("brokerName", "") or "",
                "listing_agent_email": attribution.get("agentEmail", "") or "",
                "listing_agent_phone": attribution.get("agentPhoneNumber", "") or bd_raw.get("brokerPhoneNumber", "") or "",
            }

            # Days-on-market gate (skip stale listings)
            if prop["days_on_market"] > MAX_DAYS_ON_MARKET > 0 and prop["days_on_market"] > 0:
                continue

            prop = calculate_dscr_metrics(prop)
            if prop["cash_on_cash"] >= min_coc:
                qualifying.append(prop)

        return (market, qualifying, len(props), None)

    # Parallel market fan-out. Each worker owns its own PDP-level parallelism
    # inside fetch_market, so 6 × 4 = 24 concurrent PDP fetches peak. That
    # still lives well inside Firecrawl/BrightData limits and brings a
    # 31-market run from ~2.7h → ~25–30 min.
    all_properties: List[dict] = []
    with ThreadPoolExecutor(max_workers=6) as ex:
        futures = {ex.submit(_process_market, m): m for m in markets}
        for fut in as_completed(futures):
            market, qualifying, scraped, err = fut.result()
            if err:
                log.warning(f"  {market}: {err}")
                continue
            all_properties.extend(qualifying)
            log.info(f"  {market}: {scraped} scraped → {len(qualifying)} qualifying (CoC ≥ {min_coc}%)")

    log.info(f"\nTier 1 (smart_fetch) total: {len(all_properties)} qualifying properties across {len(markets)} markets")
    return all_properties


def try_curl_cffi(markets: List[str], min_coc: float) -> List[dict]:
    """Tier 2 (fallback): Scrape Zillow using curl_cffi for better TLS fingerprinting.

    Historically Tier 1. Demoted after PX began returning 403 on all curl_cffi
    requests in April 2026; kept for the rare case where Zillow relaxes the
    block and the internal search API returns data faster than smart_fetch.
    """
    session = cf.Session(impersonate="chrome131")
    session.headers.update(BASE_HEADERS)

    log.info("Tier 1: curl_cffi — getting session cookies...")

    if SMART_FETCH_AVAILABLE:
        # smart_fetch handles curl_cffi → BrightData → Playwright automatically
        homepage = sf_fetch("https://www.zillow.com/")
        if not homepage["success"]:
            log.warning(f"All fetch tiers failed: {homepage.get('error')}")
            # Fall to BrightData fallback (existing try_brightdata_fallback)
            return []
        log.info("smart_fetch homepage succeeded; continuing with curl_cffi session for PUT calls")
        # Seed the curl_cffi session with a lightweight get so cookies are populated
        try:
            r = session.get("https://www.zillow.com/", timeout=20)
            if r.status_code != 200:
                log.warning(f"curl_cffi session cookie grab returned {r.status_code}")
        except Exception as e:
            log.warning(f"curl_cffi session cookie grab failed: {e}")
    else:
        try:
            r = session.get("https://www.zillow.com/", timeout=20)
        except Exception as e:
            log.warning(f"curl_cffi homepage request failed: {e}")
            return []

        if r.status_code != 200:
            log.warning(f"curl_cffi blocked ({r.status_code}), falling to BrightData")
            return []

    log.info(f"Got cookies: {list(session.cookies.keys())}")
    session.headers.update({"origin": "https://www.zillow.com"})

    all_properties = []

    for market in markets:
        log.info(f"\n=== {market} ===")
        bounds = MARKET_BOUNDS.get(market)
        if not bounds:
            log.warning(f"No bounds configured for {market}, skipping")
            continue

        market_properties = []
        for page in range(1, 4):  # up to 3 pages per market
            session.headers["referer"] = f"https://www.zillow.com/homes/for_sale/{market.replace(' ', '-')}_rb/"
            payload = build_search_payload(market, bounds, page)

            try:
                resp = session.put(
                    "https://www.zillow.com/async-create-search-page-state",
                    data=json.dumps(payload),
                    headers={"content-type": "application/json"},
                    timeout=30,
                )
                if resp.status_code != 200:
                    log.warning(f"API returned {resp.status_code} for {market} page {page}")
                    break

                data = resp.json()
                cat1 = data.get("cat1", {}).get("searchResults", {})
                map_results = cat1.get("mapResults", [])
                total = cat1.get("totalResultCount", 0)

                page_count = 0
                for item in map_results:
                    listing = extract_listing(item)
                    if listing:
                        listing = calculate_dscr_metrics(listing)
                        market_properties.append(listing)
                        page_count += 1

                log.info(f"  Page {page}: {page_count} valid listings (of {len(map_results)} results)")

                if len(map_results) == 0 or page * 500 >= total:
                    break

                time.sleep(random.uniform(1.5, 3.0))

            except Exception as e:
                log.error(f"Error on {market} page {page}: {e}")
                break

        log.info(f"{market}: {len(market_properties)} properties found")
        all_properties.extend(market_properties)

        if market != markets[-1]:
            time.sleep(random.uniform(2, 4))

    # Filter by min Cash-on-Cash, sort by CoC descending
    qualifying = [p for p in all_properties if p["cash_on_cash"] >= min_coc]
    qualifying.sort(key=lambda x: x["cash_on_cash"], reverse=True)

    try:
        from smart_fetch.validator import validate_property
        for prop in qualifying:
            prop["_validation"] = validate_property(prop)
    except ImportError:
        pass

    return qualifying


def try_brightdata_fallback(markets: List[str]) -> List[dict]:
    """Tier 2: When direct scraping fails, enrich existing data via BrightData."""
    existing_file = Path("/tmp/for_sale_properties.json")
    if not existing_file.exists():
        log.warning("No existing data to enrich via BrightData")
        return []

    existing = json.loads(existing_file.read_text())
    if not existing:
        return []

    # Filter to requested markets
    market_cities = {m.split()[0].lower() for m in markets}
    relevant = [p for p in existing if p.get("city", "").lower() in market_cities]

    if not relevant:
        relevant = existing  # use all if no market match

    # Try to import and use BrightData enricher
    try:
        sys.path.insert(0, "/Users/marcmunoz/n8n-workflows")
        from brightdata_zillow import enrich_via_brightdata  # type: ignore
        log.info(f"BrightData fallback: enriching {len(relevant)} existing properties")
        enriched = enrich_via_brightdata(relevant)
        return enriched
    except Exception as e:
        log.warning(f"BrightData fallback failed: {e}")
        return relevant  # return unenriched existing data as last resort


def run(markets: List[str], min_coc: float = 25.0):
    """Main entry point with tiered fallback."""
    log.info(f"Zillow For-Sale Monitor - {datetime.now().isoformat()}")
    log.info(f"Markets: {', '.join(markets)}")
    log.info(f"Min Cash-on-Cash: {min_coc}%")

    # TIER 1: smart_fetch (Firecrawl bypass of PX) — primary since Apr 2026
    properties = try_smart_fetch(markets, min_coc)

    if not properties:
        log.warning("Tier 1 (smart_fetch) returned no results, attempting Tier 2: curl_cffi")
        # TIER 2: curl_cffi with Chrome-131 TLS impersonation (historically Tier 1)
        properties = try_curl_cffi(markets, min_coc)

    if not properties:
        log.warning("Tiers 1 + 2 returned no results, attempting Tier 3: BrightData fallback (stale cache)")
        # TIER 3: BrightData enrichment of existing cached data
        properties = try_brightdata_fallback(markets)

    log.info(f"\n{'='*60}")
    log.info(f"RESULTS: {len(properties)} properties with {min_coc}%+ Cash-on-Cash")

    for i, prop in enumerate(properties[:10]):
        log.info(
            f"  #{i+1} [{prop['cash_on_cash']:.1f}% CoC] {prop['address']}, {prop['city']} {prop['state']} "
            f"- ${prop['listing_price']:,}, Rent: ${prop['rent_zestimate']}/mo, "
            f"Cash Flow: +${prop['monthly_cash_flow']:.0f}/mo, DSCR: {prop['dscr_ratio']:.2f}"
        )

    # Always save results (even partial)
    Path("/tmp/for_sale_properties.json").write_text(json.dumps(properties, indent=2))
    log.info(f"Saved {len(properties)} qualifying properties to /tmp/for_sale_properties.json")

    return properties


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Zillow For-Sale Monitor for DSCR Investment Properties")
    parser.add_argument("--markets", nargs="+", default=DEFAULT_MARKETS,
                        help="Markets to search (e.g., 'Miami FL' 'Houston TX')")
    parser.add_argument("--min-coc", type=float, default=25.0,
                        help="Minimum Cash-on-Cash return percentage")
    args = parser.parse_args()
    run(args.markets, args.min_coc)