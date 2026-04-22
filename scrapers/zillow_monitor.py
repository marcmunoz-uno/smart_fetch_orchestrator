#!/usr/bin/env python3
"""
Zillow Rental Monitor - Find overleveraged landlords who might need DSCR refinancing.

Searches Zillow rental listings for target markets and scores leads based on:
- Low rent relative to estimated property value (overleveraged indicator)
- Long days on market (struggling to find tenants)  
- Price cuts (desperate landlord)
- Asking rent below rent Zestimate (desperate pricing)

Usage:
    python3 zillow_monitor.py --markets "Indianapolis IN" "Miami FL"
    python3 zillow_monitor.py  # runs all default markets
"""

import argparse
import json
import os
import random
import re
import sys
import time
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import httpx
from loguru import logger as log

# Configure logging
log.remove()
log.add(sys.stderr, level="INFO", format="<green>{time:HH:mm:ss}</green> | <level>{level:<7}</level> | {message}")

# --- Constants ---
DEFAULT_MARKETS = [
    "Miami FL", "Houston TX", "Dallas TX", "Atlanta GA",
    "Phoenix AZ", "Indianapolis IN", "Kansas City MO", "Cleveland OH",
    "Birmingham AL", "Memphis TN", "Detroit MI", "St Louis MO",
    "Milwaukee WI", "Columbus OH", "Jacksonville FL",
    "Jackson MS", "Lorain OH", "Toledo OH", "Akron OH", "Dayton OH",
    # Added 2026-04-16 — expansion batch
    "Tampa FL", "Orlando FL", "Pittsburgh PA", "New Orleans LA",
    "Baton Rouge LA", "Tulsa OK", "Little Rock AR", "Macon GA",
    "Oklahoma City OK", "Lansing MI", "Chicago IL",
]

RENT_MIN = 800
RENT_MAX = 3500
VALUE_MIN = 100_000
VALUE_MAX = 750_000

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

SCRIPT_DIR = Path(__file__).parent
LEADS_DIR = SCRIPT_DIR / "leads"


def build_search_payload(market: str, bounds: dict, page: int = 1) -> dict:
    """Build search API payload for rental listings."""
    payload = {
        "searchQueryState": {
            "pagination": {} if page <= 1 else {"currentPage": page},
            "usersSearchTerm": market.replace(" ", ", "),  # "Indianapolis, IN"
            "mapBounds": bounds,
            "filterState": {
                "isForRent": {"value": True},
                "isForSaleByAgent": {"value": False},
                "isForSaleByOwner": {"value": False},
                "isNewConstruction": {"value": False},
                "isComingSoon": {"value": False},
                "isAuction": {"value": False},
                "isForSaleForeclosure": {"value": False},
                "isAllHomes": {"value": True},
                "monthlyPayment": {"min": RENT_MIN, "max": RENT_MAX},
                # Exclude apartments/condos - focus on houses
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
    """Extract a lead from a Zillow map/list result item."""
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

    # Price
    price_raw = item.get("price", "")
    if isinstance(price_raw, str):
        nums = re.findall(r"[\d,]+", price_raw.replace(",", ""))
        asking_rent = int(nums[0]) if nums else 0
    else:
        asking_rent = int(price_raw or 0)

    # Also check hdp price
    if not asking_rent:
        asking_rent = int(hdp.get("price", 0))

    if asking_rent < RENT_MIN or asking_rent > RENT_MAX:
        return None

    # Value
    zestimate = hdp.get("zestimate", 0) or 0
    rent_zestimate = hdp.get("rentZestimate", 0) or 0

    # Days on market from timeOnZillow (milliseconds)
    time_on = hdp.get("timeOnZillow") or item.get("timeOnZillow") or 0
    days_on_market = int(time_on / 86_400_000) if time_on > 0 else 0
    # Override with daysOnZillow if valid
    doz = hdp.get("daysOnZillow", -1)
    if doz and doz > 0:
        days_on_market = doz

    # Price change info
    price_cuts = 0
    var_data = item.get("variableData", {})
    var_text = str(var_data.get("text", "")).lower()
    if "price cut" in var_text or "price decrease" in var_text or "reduced" in var_text:
        price_cuts = 1

    # Flex field recommendations may have time info
    flex = item.get("listCardRecommendation", {}).get("flexFieldRecommendations", [])
    listing_age_text = ""
    for f in flex:
        if f.get("contentType") == "timeOnInfo":
            listing_age_text = f.get("displayString", "")

    address = item.get("address", "")
    street = hdp.get("streetAddress", "") or address.split(",")[0].strip()
    city = hdp.get("city", "")
    state = hdp.get("state", "")
    zipcode = hdp.get("zipcode", "")

    detail_url = item.get("detailUrl", "")
    if detail_url and not detail_url.startswith("http"):
        detail_url = f"https://www.zillow.com{detail_url}"

    return {
        "zpid": zpid,
        "address": street,
        "city": city,
        "state": state,
        "zipcode": zipcode,
        "asking_rent": asking_rent,
        "estimated_value": zestimate,
        "rent_zestimate": rent_zestimate,
        "tax_assessed_value": hdp.get("taxAssessedValue", 0) or 0,
        "days_on_market": days_on_market,
        "listing_age_text": listing_age_text,
        "price_cuts": price_cuts,
        "home_type": home_type,
        "beds": hdp.get("bedrooms", 0) or item.get("beds", 0),
        "baths": hdp.get("bathrooms", 0) or item.get("baths", 0),
        "sqft": hdp.get("livingArea", 0) or item.get("area", 0),
        "zillow_url": detail_url,
        "img_src": item.get("imgSrc", "") or item.get("miniCardPhotos", [{}])[0].get("url", "") if item.get("miniCardPhotos") else item.get("imgSrc", ""),
    }


def score_lead(lead: dict) -> dict:
    """Calculate DSCR score (0-10). Higher = more likely overleveraged."""
    score = 0.0
    reasons = []

    rent = lead["asking_rent"]
    value = lead["estimated_value"]
    dom = lead["days_on_market"]
    cuts = lead["price_cuts"]
    rent_zest = lead["rent_zestimate"]
    tax_val = lead["tax_assessed_value"]

    # Use best available value estimate
    est_value = value or tax_val
    if est_value:
        lead["estimated_value"] = est_value  # backfill

    # 1. Rent-to-value ratio
    if est_value and est_value >= VALUE_MIN:
        annual_yield = (rent * 12) / est_value
        if annual_yield < 0.05:
            score += 3.0
            reasons.append(f"Very low yield ({annual_yield:.1%})")
        elif annual_yield < 0.07:
            score += 2.0
            reasons.append(f"Low yield ({annual_yield:.1%})")
        elif annual_yield < 0.08:
            score += 1.0
            reasons.append(f"Below-avg yield ({annual_yield:.1%})")

    # 2. Asking rent vs rent Zestimate
    if rent_zest and rent_zest > 0:
        if rent > rent_zest * 1.15:
            score += 2.0
            reasons.append(f"Asking ${rent} vs Zest ${rent_zest} - overpriced, will sit")
        elif rent > rent_zest * 1.05:
            score += 1.0
            reasons.append(f"Slightly above rent Zestimate (${rent_zest})")
        elif rent < rent_zest * 0.90:
            score += 1.5
            reasons.append(f"Asking below rent Zestimate - desperate (${rent} vs ${rent_zest})")

    # 3. Days on market
    if dom >= 60:
        score += 2.5
        reasons.append(f"Very long DOM ({dom}d)")
    elif dom >= 30:
        score += 1.5
        reasons.append(f"Long DOM ({dom}d)")
    elif dom >= 14:
        score += 0.5
        reasons.append(f"Moderate DOM ({dom}d)")

    # 4. Price cuts
    if cuts >= 2:
        score += 2.0
        reasons.append(f"Multiple price cuts ({cuts})")
    elif cuts >= 1:
        score += 1.5
        reasons.append("Price cut detected")

    # 5. In DSCR value range
    if est_value and VALUE_MIN <= est_value <= VALUE_MAX:
        score += 0.5
        reasons.append("In DSCR loan range")

    score = round(min(score, 10.0), 1)
    lead["dscr_score"] = score
    lead["reason"] = "; ".join(reasons) if reasons else "Standard listing"
    return lead


def load_previous_zpids(leads_dir: Path) -> set:
    """Load zpids from previous runs for deduplication."""
    seen = set()
    if not leads_dir.exists():
        return seen
    for f in leads_dir.glob("*.json"):
        try:
            for lead in json.loads(f.read_text()):
                if zpid := lead.get("zpid"):
                    seen.add(str(zpid))
        except Exception:
            pass
    return seen


def save_leads(leads: List[dict], leads_dir: Path):
    """Save to dated file and /tmp."""
    leads_dir.mkdir(parents=True, exist_ok=True)
    today = date.today().isoformat()
    dated_file = leads_dir / f"{today}.json"

    # Merge with existing
    existing = []
    if dated_file.exists():
        try:
            existing = json.loads(dated_file.read_text())
        except Exception:
            pass

    existing_zpids = {l.get("zpid") for l in existing}
    for lead in leads:
        if lead.get("zpid") not in existing_zpids:
            existing.append(lead)

    dated_file.write_text(json.dumps(existing, indent=2))
    log.info(f"Saved {len(existing)} leads to {dated_file}")

    Path("/tmp/zillow_leads.json").write_text(json.dumps(existing, indent=2))
    log.info(f"Saved {len(existing)} leads to /tmp/zillow_leads.json")


def run(markets: List[str], min_score: float = 0):
    """Main entry point."""
    log.info(f"Zillow Rental Monitor - {datetime.now().isoformat()}")
    log.info(f"Markets: {', '.join(markets)}")

    previous_zpids = load_previous_zpids(LEADS_DIR)
    log.info(f"Loaded {len(previous_zpids)} previously seen zpids")

    client = httpx.Client(
        http2=True,
        headers=BASE_HEADERS,
        timeout=30.0,
        follow_redirects=True,
    )

    # Get cookies from homepage
    log.info("Getting session cookies...")
    r = client.get("https://www.zillow.com/")
    if r.status_code != 200:
        log.error(f"Could not load Zillow homepage: {r.status_code}")
        return []
    log.info(f"Got cookies: {list(client.cookies.keys())}")

    # Set referer/origin for API calls
    client.headers.update({
        "origin": "https://www.zillow.com",
    })

    all_leads = []

    for market in markets:
        log.info(f"\n=== {market} ===")
        bounds = MARKET_BOUNDS.get(market)
        if not bounds:
            log.warning(f"No bounds configured for {market}, skipping")
            continue

        market_leads = []
        for page in range(1, 6):  # up to 5 pages
            client.headers["referer"] = f"https://www.zillow.com/homes/for_rent/{market.replace(' ', '-')}_rb/"
            payload = build_search_payload(market, bounds, page)

            try:
                resp = client.put(
                    "https://www.zillow.com/async-create-search-page-state",
                    content=json.dumps(payload),
                    headers={"content-type": "application/json"},
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
                        listing = score_lead(listing)
                        market_leads.append(listing)
                        page_count += 1

                log.info(f"  Page {page}: {page_count} valid listings (of {len(map_results)} results)")

                if len(map_results) == 0 or page * 500 >= total:
                    break

                time.sleep(random.uniform(1.5, 3.0))

            except Exception as e:
                log.error(f"Error on {market} page {page}: {e}")
                break

        # Dedup within market
        seen = set()
        unique = []
        for l in market_leads:
            if l["zpid"] not in seen:
                seen.add(l["zpid"])
                unique.append(l)
        market_leads = unique

        # Filter out previously seen
        new = [l for l in market_leads if l["zpid"] not in previous_zpids]
        log.info(f"{market}: {len(new)} new leads (of {len(market_leads)} total)")
        all_leads.extend(new)

        if market != markets[-1]:
            time.sleep(random.uniform(2, 4))

    client.close()

    # Filter by min score, sort
    quality = [l for l in all_leads if l["dscr_score"] >= min_score]
    quality.sort(key=lambda x: x["dscr_score"], reverse=True)

    log.info(f"\n{'='*60}")
    log.info(f"RESULTS: {len(quality)} leads (score >= {min_score})")

    for i, lead in enumerate(quality[:25]):
        val_str = f"${lead['estimated_value']:,}" if lead['estimated_value'] else "N/A"
        log.info(
            f"  #{i+1} [{lead['dscr_score']}] {lead['address']}, {lead['city']} {lead['state']} "
            f"- ${lead['asking_rent']}/mo, Val: {val_str}, DOM: {lead['days_on_market']}d"
        )
        log.info(f"       {lead['reason']}")

    if quality:
        save_leads(quality, LEADS_DIR)
    else:
        log.info("No leads found this run")

    return quality


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Zillow Rental Monitor for DSCR Leads")
    parser.add_argument("--markets", nargs="+", default=DEFAULT_MARKETS,
                        help="Markets to monitor (e.g., 'Miami FL' 'Houston TX')")
    parser.add_argument("--min-score", type=float, default=0,
                        help="Minimum DSCR score to include")
    args = parser.parse_args()
    run(args.markets, args.min_score)

# Additional market bounds added 2026-02-23
MARKET_BOUNDS.update({
    "Jackson MS":       {"west": -90.30, "east": -90.05, "south": 32.25, "north": 32.40},
    "Lorain OH":        {"west": -82.25, "east": -82.10, "south": 41.43, "north": 41.48},
    "Toledo OH":        {"west": -83.65, "east": -83.45, "south": 41.62, "north": 41.72},
    "Akron OH":         {"west": -81.60, "east": -81.45, "south": 41.04, "north": 41.12},
    "Dayton OH":        {"west": -84.25, "east": -84.10, "south": 39.72, "north": 39.82},
})
