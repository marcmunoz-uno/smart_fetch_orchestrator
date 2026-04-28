"""ATTOM Property Data enricher — gold-standard parcel + AVM + sale history.

Uses ATTOM's /property/expandedprofile (single call covers identifier, address,
assessment, sale, building, summary, location, area, lot) and optionally
/avm/snapshot for live valuation.

Trial tier confirmed working endpoints (Apr 27, 2026):
  ✓ /property/basicprofile
  ✓ /property/expandedprofile     <- preferred: 1 call covers most fields
  ✓ /property/detail
  ✓ /avm/snapshot                 <- separate call for AVM with confidence
  ✓ /assessment/detail
  ✓ /sale/snapshot
  ✓ /saleshistory/detail
  ✗ /school/search                404 on trial
  ✗ /salestrend/snapshot          404 on trial (or needs valid geoIdV4)

Conservative defaults: 1 call per property (expandedprofile only). Pass
include_avm=True to add AVM snapshot (2nd call, gets confidence + range).
include_sale_history=True adds full sale history (3rd call).

Standard field mapping into property dict — all ATTOM-derived fields are
prefixed `attom_` so they live alongside HC/BD/BatchData fields without
clobbering them.
"""

import time
import requests
from typing import Optional

from smart_fetch.config import ATTOM_API_KEY, ATTOM_BASE, RATE_LIMITS
from smart_fetch.utils.api_cache import get_cache

_RATE = RATE_LIMITS["attom"]
_CACHE_NS = "attom"

# Per-endpoint TTLs in seconds. ATTOM data refreshes on assessor cycles
# (quarterly), AVMs monthly, transactions/permits only when filed. Cache
# aggressively; the trial quota is the binding constraint, not staleness.
_DAY = 86400
TTL_BY_ENDPOINT = {
    # Property identity / characteristics — change essentially never
    "/property/basicprofile":      90 * _DAY,
    "/property/expandedprofile":   90 * _DAY,
    "/property/detail":            90 * _DAY,
    "/property/snapshot":          90 * _DAY,
    "/property/detailowner":       30 * _DAY,
    "/property/detailmortgage":    30 * _DAY,
    "/property/buildingpermits":   60 * _DAY,
    # AVM — monthly cycle; refresh after 30 days
    "/avm/snapshot":               30 * _DAY,
    "/avm/detail":                 30 * _DAY,
    "/avmhistory/detail":          30 * _DAY,
    # Assessment / tax — yearly
    "/assessment/detail":          90 * _DAY,
    "/assessment/snapshot":        90 * _DAY,
    "/assessmenthistory/detail":   90 * _DAY,
    # Sale / transactions — only changes when sold
    "/sale/snapshot":              60 * _DAY,
    "/sale/detail":                60 * _DAY,
    "/saleshistory/detail":        60 * _DAY,
    "/saleshistory/expandedhistory": 60 * _DAY,
}
DEFAULT_TTL = 30 * _DAY


def _headers():
    return {"APIKey": ATTOM_API_KEY, "Accept": "application/json"}


def _no_key():
    return {"_attom_error": "ATTOM_API_KEY not set"}


def _is_success(payload: dict) -> bool:
    """ATTOM returns 200 + status.msg='SuccessWithResult' on real hits."""
    if not isinstance(payload, dict):
        return False
    if "_error" in payload:
        return False
    status = payload.get("status") or payload.get("Response", {}).get("status", {})
    msg = (status or {}).get("msg", "") if isinstance(status, dict) else ""
    return "Success" in str(msg)


def _get(path: str, params: dict, timeout: int = 20, *, use_cache: bool = True) -> dict:
    """ATTOM GET with SQLite cache. Cache-first; misses fetch and store on success only."""
    cache = get_cache() if use_cache else None

    if cache:
        cached = cache.get(_CACHE_NS, path, params)
        if cached is not None:
            cached.setdefault("_cache_hit", True)
            return cached

    try:
        resp = requests.get(f"{ATTOM_BASE}{path}", headers=_headers(), params=params, timeout=timeout)
    except requests.exceptions.Timeout:
        return {"_error": f"timed out after {timeout}s"}
    except Exception as exc:
        return {"_error": str(exc)}
    finally:
        time.sleep(_RATE["delay_s"])

    # ATTOM quirk: returns HTTP 400 with `msg: SuccessWithoutResult` when a
    # property has no data for that endpoint. The body still parses; cache it
    # so we don't keep re-hitting the API for a known-empty property.
    try:
        body = resp.json()
    except Exception:
        if resp.status_code != 200:
            return {"_error": f"HTTP {resp.status_code}: {resp.text[:200]}"}
        return {"_error": "JSON parse failure"}

    # 401/403/429/5xx are real errors — don't cache; surface to caller.
    if resp.status_code in (401, 403, 429) or resp.status_code >= 500:
        return {"_error": f"HTTP {resp.status_code}: {resp.text[:200]}"}

    # Cache anything ATTOM considers a "Success" (with or without result).
    if cache and _is_success(body):
        ttl = TTL_BY_ENDPOINT.get(path, DEFAULT_TTL)
        cache.put(_CACHE_NS, path, body, params=params, ttl_seconds=ttl)

    return body


def cache_stats() -> dict:
    """Return ATTOM cache stats — entry counts, hits, freshness per endpoint."""
    return get_cache().stats(namespace=_CACHE_NS)


def invalidate_cache(endpoint: Optional[str] = None,
                     params: Optional[dict] = None) -> int:
    """Drop ATTOM cache entries. No args → drop everything ATTOM-namespaced."""
    return get_cache().invalidate(_CACHE_NS, endpoint=endpoint, params=params)


def _addr_params(prop: dict) -> Optional[dict]:
    """Build the address1/address2 params from a property dict."""
    a1 = prop.get("address") or prop.get("streetAddress") or prop.get("line1")
    city = prop.get("city") or prop.get("locality")
    state = prop.get("state") or prop.get("countrySubd")
    zipc = prop.get("zipcode") or prop.get("zip_code") or prop.get("postal1")
    if not a1:
        return None
    a2 = ", ".join(p for p in (city, state) if p)
    if zipc:
        a2 = f"{a2} {zipc}".strip()
    return {"address1": str(a1), "address2": a2}


def _flatten_expanded(prop_response: dict) -> dict:
    """Flatten the rich expandedprofile shape into attom_-prefixed flat fields."""
    out = {}
    if not prop_response or "_error" in prop_response:
        return out
    plist = prop_response.get("property", [])
    if not plist:
        return out
    p = plist[0] if isinstance(plist, list) else plist

    # Identifiers
    ident = p.get("identifier", {}) or {}
    out["attom_id"] = ident.get("attomId") or ident.get("Id")
    out["attom_apn"] = ident.get("apn")
    out["attom_fips"] = ident.get("fips")

    # Address (canonical)
    addr = p.get("address", {}) or {}
    out["attom_address"] = addr.get("oneLine") or addr.get("line1")
    out["attom_match_code"] = addr.get("matchCode")

    # Location + census
    loc = p.get("location", {}) or {}
    out["attom_lat"] = loc.get("latitude")
    out["attom_lon"] = loc.get("longitude")
    out["attom_geoid"] = loc.get("geoid")
    out["attom_geoidv4"] = loc.get("geoIdV4")
    area = p.get("area", {}) or {}
    out["attom_census_tract"] = area.get("censusTractIdent")
    out["attom_census_blockgroup"] = area.get("censusBlockGroup")
    out["attom_county_use"] = area.get("countyUse1")
    out["attom_munname"] = area.get("munName")

    # Summary — high-signal flags
    summary = p.get("summary", {}) or {}
    out["attom_property_type"] = summary.get("propertyType") or summary.get("propType")
    out["attom_property_subtype"] = summary.get("propSubType")
    out["attom_year_built"] = summary.get("yearBuilt")
    out["attom_arch_style"] = summary.get("archStyle")
    out["attom_legal"] = summary.get("legal1")
    # absentee/REO are the gold flags
    abs_ind = summary.get("absenteeInd")
    out["attom_absentee_owner"] = abs_ind not in (None, "", "OWNER OCCUPIED")
    out["attom_absentee_raw"] = abs_ind
    out["attom_reo_flag"] = bool(summary.get("REOflag"))
    out["attom_quitclaim_flag"] = bool(summary.get("quitClaimFlag"))

    # Building characteristics
    building = p.get("building", {}) or {}
    size = building.get("size", {}) or {}
    out["attom_sqft"] = size.get("livingSize") or size.get("universalSize") or size.get("bldgSize")
    rooms = building.get("rooms", {}) or {}
    out["attom_beds"] = rooms.get("beds")
    out["attom_baths"] = rooms.get("bathstotal") or rooms.get("bathsFull")
    interior = building.get("interior", {}) or {}
    out["attom_basement"] = interior.get("bsmtType")
    construction = building.get("construction", {}) or {}
    out["attom_construction_type"] = construction.get("wallType") or construction.get("constructionType")
    out["attom_condition"] = construction.get("condition")
    parking = building.get("parking", {}) or {}
    out["attom_garage_type"] = parking.get("garageType")
    out["attom_parking_spaces"] = parking.get("prkgSpaces")

    # Lot
    lot = p.get("lot", {}) or {}
    out["attom_lot_size_sqft"] = lot.get("lotSize2")
    out["attom_lot_size_acres"] = lot.get("lotSize1")
    out["attom_zoning"] = lot.get("zoningType") or lot.get("siteZoningIdent")

    # Assessment — values + tax + owner + mortgage
    assess = p.get("assessment", {}) or {}
    appraised = assess.get("appraised", {}) or {}
    out["attom_appraised_total"] = appraised.get("apprTtlValue")
    assessed = assess.get("assessed", {}) or {}
    out["attom_assessed_total"] = assessed.get("assdTtlValue")
    out["attom_assessed_land"] = assessed.get("assdLandValue")
    out["attom_assessed_imprv"] = assessed.get("assdImprValue")
    market = assess.get("market", {}) or {}
    out["attom_market_value"] = market.get("mktTtlValue")
    out["attom_market_year"] = market.get("mktTtlYear")
    tax = assess.get("tax", {}) or {}
    out["attom_tax_amt"] = tax.get("taxAmt")
    out["attom_tax_year"] = tax.get("taxYear")
    owner = assess.get("owner", {}) or {}
    # ATTOM owner is structured — assemble names
    onames = []
    for k in ("owner1", "owner2", "owner3", "owner4"):
        ow = owner.get(k, {}) or {}
        nm = " ".join(p for p in (ow.get("firstNameAndMi"), ow.get("lastName")) if p).strip()
        if nm:
            onames.append(nm)
    out["attom_owner_names"] = onames
    out["attom_owner_corporate"] = owner.get("corporateIndicator") == "Y"
    mortgage = assess.get("mortgage", {}) or {}
    title1 = (mortgage.get("title", {}) or {}).get("titleCompany")
    out["attom_mortgage_title_company"] = title1
    # First mortgage
    fm = mortgage.get("FirstConcurrent", {}) or {}
    out["attom_mortgage_amount"] = (fm.get("amount") or {}).get("amount") if isinstance(fm.get("amount"), dict) else fm.get("amount")
    out["attom_mortgage_lender"] = fm.get("lender")
    out["attom_mortgage_date"] = fm.get("date")
    out["attom_mortgage_term"] = fm.get("term")
    out["attom_mortgage_type"] = fm.get("trustDeedType")

    # Last sale (single — full history needs separate call)
    sale = p.get("sale", {}) or {}
    out["attom_last_sale_date"] = sale.get("saleTransDate")
    samount = sale.get("amount", {}) or {}
    out["attom_last_sale_price"] = samount.get("saleAmt")
    out["attom_last_sale_seller"] = sale.get("sellerName")

    return out


def _flatten_avm(avm_response: dict) -> dict:
    out = {}
    if not avm_response or "_error" in avm_response:
        return out
    plist = avm_response.get("property", [])
    if not plist:
        return out
    p = plist[0] if isinstance(plist, list) else plist
    avm = p.get("avm", {}) or {}
    amount = avm.get("amount", {}) or {}
    out["attom_avm"] = amount.get("value")
    out["attom_avm_low"] = amount.get("low")
    out["attom_avm_high"] = amount.get("high")
    out["attom_avm_confidence"] = amount.get("scr")
    out["attom_avm_date"] = avm.get("eventDate")
    return out


def _flatten_sale_history(sh_response: dict) -> dict:
    out = {"attom_sale_history": []}
    if not sh_response or "_error" in sh_response:
        return out
    plist = sh_response.get("property", [])
    if not plist:
        return out
    p = plist[0] if isinstance(plist, list) else plist
    history = p.get("salehistory", []) or []
    for s in (history if isinstance(history, list) else []):
        amt = s.get("amount", {}) or {}
        out["attom_sale_history"].append({
            "date": s.get("saleTransDate"),
            "price": amt.get("saleAmt"),
            "seller": s.get("sellerName"),
            "doc_type": s.get("transactionIdent"),
        })
    return out


def enrich(
    prop: dict,
    *,
    include_avm: bool = True,
    include_sale_history: bool = False,
) -> dict:
    """Enrich a property dict with ATTOM data.

    Default: 2 API calls (expandedprofile + avm) per property.
    Set include_avm=False for 1 call.
    Set include_sale_history=True for 3rd call (full history list).

    All ATTOM fields are prefixed `attom_` and merged into prop. Original
    keys are preserved. Errors are surfaced under `_attom_error`.
    """
    if not ATTOM_API_KEY:
        prop.update(_no_key())
        return prop

    params = _addr_params(prop)
    if not params:
        prop["_attom_error"] = "no address available for ATTOM lookup"
        return prop

    # Primary: expanded profile
    exp = _get("/property/expandedprofile", params)
    if "_error" in exp:
        prop["_attom_error"] = f"expandedprofile: {exp['_error']}"
        return prop
    prop.update(_flatten_expanded(exp))

    # Optional: AVM
    if include_avm:
        avm = _get("/avm/snapshot", params)
        if "_error" not in avm:
            prop.update(_flatten_avm(avm))

    # Optional: Full sale history
    if include_sale_history:
        sh = _get("/saleshistory/detail", params)
        if "_error" not in sh:
            prop.update(_flatten_sale_history(sh))

    return prop


def lookup_by_attom_id(attom_id: str, *, include_avm: bool = True) -> dict:
    """Direct lookup by attomId (most efficient when you already have it)."""
    if not ATTOM_API_KEY:
        return _no_key()
    out = {}
    exp = _get("/property/expandedprofile", {"attomid": attom_id})
    if "_error" not in exp:
        out.update(_flatten_expanded(exp))
    if include_avm:
        avm = _get("/avm/snapshot", {"attomid": attom_id})
        if "_error" not in avm:
            out.update(_flatten_avm(avm))
    return out
