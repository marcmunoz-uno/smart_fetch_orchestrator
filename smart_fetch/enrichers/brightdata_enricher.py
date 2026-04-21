"""BrightData enricher — pulls live Zillow listing data via MCP tool."""
from smart_fetch.fetchers.brightdata_fetcher import fetch_zillow_listing

def enrich(prop: dict) -> dict:
    """Enrich property with live Zillow data via BrightData."""
    url = prop.get("zillow_url") or prop.get("hdpUrl") or prop.get("listing_url")
    if not url or "zillow.com/homedetails/" not in str(url):
        return prop

    result = fetch_zillow_listing(url)
    if not result.get("success"):
        return prop

    data = result.get("json", {})
    if not data:
        return prop

    # Map BrightData fields to standard property fields
    mapping = {
        "rent_zestimate": "rentZestimate",
        "listing_price": "price",
        "beds": "bedrooms",
        "baths": "bathrooms",
        "sqft": "livingArea",
        "year_built": "yearBuilt",
        "home_type": "homeType",
        "photo_count": "photoCount",
        "zestimate": "zestimate",
        "tax_assessed_value": "taxAssessedValue",
        "last_sold_price": "lastSoldPrice",
        "description": "description",
        "lot_size": "lotSize",
    }

    for our_key, bd_key in mapping.items():
        val = data.get(bd_key)
        if val is not None:
            prop[f"bd_{our_key}"] = val
            # Also update the primary field if it's empty
            if not prop.get(our_key):
                prop[our_key] = val

    prop["_bd_raw"] = data
    return prop
