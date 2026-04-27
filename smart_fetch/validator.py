"""Three-source property data validation."""

from typing import Dict, List, Tuple, Optional
from datetime import datetime
from smart_fetch.config import VALIDATION


def validate_property(prop: dict) -> dict:
    """
    Cross-validate property data from multiple sources.

    Expects a property dict with fields from:
    - Zillow/BrightData: listing_price, rent_zestimate, beds, baths, sqft
    - HouseCanary: hc_avm_mean, hc_rent_mean, hc_beds, hc_baths, hc_sqft
    - BatchData (if lookup available): bd_tax_assessed, bd_beds, bd_baths

    Returns dict with:
    - validated: bool (True if data passes cross-validation)
    - confidence: float 0-1 (how reliable is this data?)
    - flags: list of warning strings
    - best_price: consensus price from available sources
    - best_rent: consensus rent from available sources
    - sources_used: list of source names that contributed
    - validated_at: ISO timestamp
    """
    flags = []
    sources_used = []

    # Split price estimates by kind: market-value sources (Zillow list price,
    # HC AVM) are commensurable; tax-assessed (BatchData) is NOT — FL's
    # Save-Our-Homes cap keeps tax-assessed at 40-60% of market, so mixing
    # them produces spurious `price_divergence` flags on every FL property.
    # Tax-assessed is kept for sources_used + as a fallback for best_price.
    market_prices = {}
    if prop.get("listing_price") and prop["listing_price"] > 0:
        market_prices["zillow"] = prop["listing_price"]
    if prop.get("hc_avm_mean") and prop["hc_avm_mean"] > 0:
        market_prices["housecanary"] = prop["hc_avm_mean"]

    tax_assessed = None
    if prop.get("bd_tax_assessed") and prop["bd_tax_assessed"] > 0:
        tax_assessed = prop["bd_tax_assessed"]

    # Keep `prices` populated for any downstream code that still reads it
    # (e.g. for_sale_rental_detector.py) — includes tax for backward compat.
    prices = dict(market_prices)
    if tax_assessed is not None:
        prices["batchdata"] = tax_assessed

    # Collect rent estimates
    rents = {}
    if prop.get("rent_zestimate") and prop["rent_zestimate"] > 0:
        rents["zillow"] = prop["rent_zestimate"]
    if prop.get("hc_rent_mean") and prop["hc_rent_mean"] > 0:
        rents["housecanary"] = prop["hc_rent_mean"]

    # Collect beds/baths for cross-check
    beds_sources = {}
    if prop.get("beds"): beds_sources["zillow"] = prop["beds"]
    if prop.get("hc_beds"): beds_sources["housecanary"] = prop["hc_beds"]

    baths_sources = {}
    if prop.get("baths"): baths_sources["zillow"] = prop["baths"]
    if prop.get("hc_baths"): baths_sources["housecanary"] = prop["hc_baths"]

    # --- Price validation (market sources only; tax-assessed is reference) ---
    best_price = None
    if market_prices:
        sources_used.extend(market_prices.keys())
        values = list(market_prices.values())
        best_price = _weighted_average(market_prices)

        if len(values) >= 2:
            divergence = (max(values) - min(values)) / min(values) * 100
            if divergence > VALIDATION["price_divergence_pct"]:
                flags.append(f"price_divergence_{divergence:.0f}pct: {market_prices}")

            # Special flag: listing price way below AVM (could be incredible deal OR bad data)
            if "zillow" in market_prices and "housecanary" in market_prices:
                listing = market_prices["zillow"]
                avm = market_prices["housecanary"]
                if listing < avm * 0.5:
                    flags.append(f"listing_50pct_below_avm: ${listing:,} vs AVM ${avm:,}")
                elif listing > avm * 1.3:
                    flags.append(f"listing_30pct_above_avm: ${listing:,} vs AVM ${avm:,}")
    elif tax_assessed:
        # No market sources — fall back to tax-assessed for best_price only.
        best_price = tax_assessed

    # Track tax-assessed as a contributing source without using it for divergence.
    if tax_assessed is not None:
        sources_used.append("batchdata")
        # Sanity flag: if tax_assessed is more than 2x any market estimate,
        # something is probably wrong (wrong property matched, not SOH cap).
        if market_prices:
            max_market = max(market_prices.values())
            if tax_assessed > max_market * 2:
                flags.append(
                    f"tax_far_above_market: tax ${tax_assessed:,} vs max market ${max_market:,}"
                )

    # --- Rent validation ---
    best_rent = None
    if rents:
        sources_used.extend(rents.keys())
        values = list(rents.values())
        best_rent = _weighted_average(rents)

        if len(values) >= 2:
            divergence = (max(values) - min(values)) / min(values) * 100
            if divergence > VALIDATION["rent_divergence_pct"]:
                flags.append(f"rent_divergence_{divergence:.0f}pct: {rents}")

    # --- Beds/baths cross-check ---
    if len(beds_sources) >= 2:
        if len(set(beds_sources.values())) > 1:
            flags.append(f"beds_mismatch: {beds_sources}")
    if len(baths_sources) >= 2:
        if len(set(int(v) for v in baths_sources.values())) > 1:
            flags.append(f"baths_mismatch: {baths_sources}")

    # --- Flood risk ---
    if prop.get("hc_flood_zone") and prop["hc_flood_zone"] not in ("X", "C", None):
        flags.append(f"flood_zone_{prop['hc_flood_zone']}: may need flood insurance")

    # --- Distress signals ---
    if prop.get("hc_nod_flag"):
        flags.append("notice_of_default: owner in pre-foreclosure")
    if prop.get("hc_ltv") and prop["hc_ltv"] > 0.9:
        flags.append(f"high_ltv_{prop['hc_ltv']:.0%}: owner likely underwater")
    if prop.get("hc_owner_occupied") is False:
        flags.append("non_owner_occupied: absentee investor")

    # --- Confidence score ---
    unique_sources = set(sources_used)
    source_count = len(unique_sources)

    # Base confidence from source count
    if source_count >= 3:
        confidence = 0.95
    elif source_count == 2:
        confidence = 0.75
    elif source_count == 1:
        confidence = 0.50
    else:
        confidence = 0.0

    # Penalize for flags
    penalty_per_flag = 0.08
    confidence = max(0.0, confidence - len(flags) * penalty_per_flag)

    # Bonus for matching beds/baths
    if len(beds_sources) >= 2 and len(set(beds_sources.values())) == 1:
        confidence = min(1.0, confidence + 0.05)

    validated = (
        source_count >= VALIDATION["min_sources_for_verified"]
        and confidence >= 0.60
        and not any("mismatch" in f for f in flags)
    )

    return {
        "validated": validated,
        "confidence": round(confidence, 3),
        "flags": flags,
        "best_price": round(best_price) if best_price else None,
        "best_rent": round(best_rent) if best_rent else None,
        "sources_used": sorted(unique_sources),
        "source_count": source_count,
        "validated_at": datetime.utcnow().isoformat() + "Z",
    }


def _weighted_average(source_values: Dict[str, float]) -> float:
    """Compute weighted average using source reliability weights from config."""
    weights = VALIDATION["confidence_weights"]
    total_weight = 0
    weighted_sum = 0

    for source, value in source_values.items():
        # Map source names to config keys
        key = source
        if source == "zillow":
            key = "brightdata_zillow"  # Zillow data comes via BrightData

        weight = weights.get(key, 0.2)  # default weight for unknown sources
        weighted_sum += value * weight
        total_weight += weight

    return weighted_sum / total_weight if total_weight > 0 else 0
