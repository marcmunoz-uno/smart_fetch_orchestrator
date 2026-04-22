#!/usr/bin/env python3
"""
One-off seed script for expansion markets (2026-04-16).

The httpx-based Zillow scraper is TLS-fingerprinted and returning 403.
This script reuses the parse/calc logic but uses curl_cffi (chrome131
impersonation) which passes Zillow's anti-bot. Output format is identical
to /tmp/for_sale_properties.json so the existing bridge works unchanged.

Caps at 20 qualifying properties per market (no CoC floor — we take top 20
sorted by CoC desc regardless of positive/negative cash flow).
"""
import json
import random
import sys
import time
from pathlib import Path

from curl_cffi import requests as cf

# Reuse parse + metric logic from the existing scraper
sys.path.insert(0, str(Path(__file__).parent))
from zillow_for_sale import (
    MARKET_BOUNDS,
    build_search_payload,
    calculate_dscr_metrics,
    extract_listing,
)

NEW_MARKETS = [
    "Tampa FL", "Orlando FL", "Pittsburgh PA", "New Orleans LA",
    "Baton Rouge LA", "Tulsa OK", "Little Rock AR", "Macon GA",
    "Oklahoma City OK", "Lansing MI", "Chicago IL",
]
PER_MARKET_CAP = 20
OUT_PATH = Path("/tmp/for_sale_properties.json")


def main():
    session = cf.Session(impersonate="chrome131")

    # Prime cookies
    r = session.get("https://www.zillow.com/", timeout=20)
    print(f"[init] homepage: {r.status_code}, cookies: {list(r.cookies.keys())[:5]}")
    if r.status_code != 200:
        sys.exit(1)

    session.headers.update({"origin": "https://www.zillow.com"})

    all_props = []
    per_market_counts = {}

    for market in NEW_MARKETS:
        bounds = MARKET_BOUNDS.get(market)
        if not bounds:
            print(f"[skip] {market}: no bounds")
            continue

        market_props = []
        for page in range(1, 4):
            session.headers["referer"] = (
                f"https://www.zillow.com/homes/for_sale/{market.replace(' ', '-')}_rb/"
            )
            payload = build_search_payload(market, bounds, page)
            try:
                resp = session.put(
                    "https://www.zillow.com/async-create-search-page-state",
                    data=json.dumps(payload),
                    headers={"content-type": "application/json"},
                    timeout=25,
                )
            except Exception as e:
                print(f"[err] {market} p{page}: {e}")
                break

            if resp.status_code != 200:
                print(f"[warn] {market} p{page}: status {resp.status_code}")
                break

            data = resp.json()
            cat1 = data.get("cat1", {}).get("searchResults", {})
            map_results = cat1.get("mapResults", [])
            total = cat1.get("totalResultCount", 0)

            page_valid = 0
            for item in map_results:
                listing = extract_listing(item)
                if listing:
                    listing = calculate_dscr_metrics(listing)
                    market_props.append(listing)
                    page_valid += 1

            print(f"  {market} p{page}: {page_valid}/{len(map_results)} valid (total {total})")

            if not map_results or page * 500 >= total:
                break
            time.sleep(random.uniform(1.5, 3.0))

        # Top N by CoC (descending)
        market_props.sort(key=lambda p: p["cash_on_cash"], reverse=True)
        capped = market_props[:PER_MARKET_CAP]
        per_market_counts[market] = (len(market_props), len(capped))
        all_props.extend(capped)

        # Inter-market politeness
        if market != NEW_MARKETS[-1]:
            time.sleep(random.uniform(2.5, 4.5))

    # Merge with any existing for_sale_properties.json (dedup by zpid)
    existing = []
    if OUT_PATH.exists():
        try:
            existing = json.loads(OUT_PATH.read_text())
        except Exception:
            existing = []
    existing_zpids = {p.get("zpid") for p in existing}
    new_only = [p for p in all_props if p.get("zpid") not in existing_zpids]

    merged = existing + new_only
    OUT_PATH.write_text(json.dumps(merged, indent=2))

    print("\n=== SUMMARY ===")
    for market, (raw, capped) in per_market_counts.items():
        print(f"  {market:20s}  raw={raw:3d}  kept={capped:3d}")
    print(f"\nNew properties added: {len(new_only)}")
    print(f"Total in {OUT_PATH}: {len(merged)}")


if __name__ == "__main__":
    main()
