"""HouseCanary enricher — AVM, rental estimates, flood, NOD, LTV, owner-occupied."""
import json, time, requests
from smart_fetch.config import HOUSECANARY_API_KEY, HOUSECANARY_API_SECRET, HOUSECANARY_BASE, RATE_LIMITS

AUTH = (HOUSECANARY_API_KEY, HOUSECANARY_API_SECRET)

def _post(endpoint, addresses):
    """POST batch to HouseCanary v2. addresses = [{"address": "...", "zipcode": "..."}]"""
    try:
        resp = requests.post(
            f"{HOUSECANARY_BASE}/{endpoint}",
            auth=AUTH,
            headers={"Content-Type": "application/json"},
            json=addresses,
            timeout=30)
        if resp.status_code != 200:
            return []
        return resp.json()
    except:
        return []

def enrich(prop: dict) -> dict:
    """Enrich a single property with HouseCanary data. Returns updated prop dict."""
    address = prop.get("address", "")
    zipcode = str(prop.get("zipcode") or prop.get("zip") or "")
    if not address or not zipcode:
        return prop

    payload = [{"address": address, "zipcode": zipcode}]

    # Value
    for item in _post("property/value", payload):
        result = item.get("property/value", {}).get("result", {})
        if result:
            val = result.get("value", {})
            prop["hc_avm_mean"] = val.get("price_mean")
            prop["hc_avm_low"] = val.get("price_lwr")
            prop["hc_avm_high"] = val.get("price_upr")
    time.sleep(RATE_LIMITS["housecanary"]["delay_s"])

    # Rental value
    for item in _post("property/rental_value", payload):
        result = item.get("property/rental_value", {}).get("result", {})
        if result:
            prop["hc_rent_mean"] = result.get("price_mean")
            prop["hc_rent_low"] = result.get("price_lwr")
            prop["hc_rent_high"] = result.get("price_upr")
    time.sleep(RATE_LIMITS["housecanary"]["delay_s"])

    # Flood
    for item in _post("property/flood", payload):
        result = item.get("property/flood", {}).get("result", {})
        if result:
            prop["hc_flood_zone"] = result.get("zone")
    time.sleep(RATE_LIMITS["housecanary"]["delay_s"])

    # NOD
    for item in _post("property/nod", payload):
        ep = item.get("property/nod", {})
        if ep.get("api_code") == 0:
            result = ep.get("result", [])
            prop["hc_nod_flag"] = bool(result)
    time.sleep(RATE_LIMITS["housecanary"]["delay_s"])

    # Owner occupied
    for item in _post("property/owner_occupied", payload):
        result = item.get("property/owner_occupied", {}).get("result", {})
        if result:
            prop["hc_owner_occupied"] = result.get("owner_occupied")
    time.sleep(RATE_LIMITS["housecanary"]["delay_s"])

    # LTV
    for item in _post("property/ltv_details", payload):
        result = item.get("property/ltv_details", {}).get("result", {})
        if result:
            prop["hc_ltv"] = result.get("ltv")
            prop["hc_equity"] = result.get("equity")

    return prop
