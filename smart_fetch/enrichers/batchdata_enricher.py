"""BatchData enricher — skip trace (owner contact) + phone verification."""
import requests, time
from smart_fetch.config import BATCHDATA_API_KEY, BATCHDATA_BASE, RATE_LIMITS

def skip_trace(prop: dict) -> dict:
    """Skip trace a property to find owner contact info."""
    address = prop.get("address", "")
    city = prop.get("city", "")
    state = prop.get("state", "")
    zipcode = str(prop.get("zipcode") or prop.get("zip") or "")

    if not address or not city or not state:
        return prop

    try:
        resp = requests.post(
            f"{BATCHDATA_BASE}/property/skip-trace",
            headers={"Authorization": f"Bearer {BATCHDATA_API_KEY}", "Content-Type": "application/json"},
            json={"requests": [{"propertyAddress": {"street": address, "city": city, "state": state, "zip": zipcode}}]},
            timeout=30)

        if resp.status_code != 200:
            return prop

        data = resp.json()
        persons = data.get("results", {}).get("persons", [])
        if not persons:
            return prop

        person = persons[0]
        meta = person.get("meta", {})
        if not meta.get("matched"):
            return prop

        name = person.get("name", {})
        prop["owner_name"] = name.get("full", "")
        prop["owner_first"] = name.get("first", "")
        prop["owner_last"] = name.get("last", "")

        phones = person.get("phones", [])
        if phones:
            prop["owner_phone"] = phones[0].get("phone", "")
            prop["owner_phones"] = [p.get("phone", "") for p in phones[:3]]

        emails = person.get("emails", [])
        if emails:
            prop["owner_email"] = emails[0].get("email", "")
            prop["owner_emails"] = [e.get("email", "") for e in emails[:3]]

        mailing = person.get("addresses", [])
        if mailing:
            prop["owner_mailing_address"] = mailing[0].get("full", "")

        time.sleep(RATE_LIMITS["batchdata"]["delay_s"])
    except Exception:
        pass

    return prop

def verify_phone(phone: str) -> dict:
    """Verify a phone number — line type, DNC, TCPA status."""
    try:
        resp = requests.post(
            f"{BATCHDATA_BASE}/phone/verify",
            headers={"Authorization": f"Bearer {BATCHDATA_API_KEY}", "Content-Type": "application/json"},
            json={"requests": [{"phone": phone}]},
            timeout=15)
        if resp.status_code != 200:
            return {"verified": False, "error": f"HTTP {resp.status_code}"}

        data = resp.json()
        results = data.get("results", {}).get("phones", [])
        if not results:
            return {"verified": False, "error": "no result"}

        r = results[0]
        return {
            "verified": True,
            "line_type": r.get("lineType"),
            "carrier": r.get("carrier"),
            "dnc": r.get("dnc", False),
            "tcpa_litigator": r.get("tcpaLitigator", False),
            "safe_to_call": not r.get("dnc", True) and not r.get("tcpaLitigator", True),
        }
    except Exception as e:
        return {"verified": False, "error": str(e)}

def enrich(prop: dict) -> dict:
    """Full BatchData enrichment: skip trace + phone verify."""
    prop = skip_trace(prop)

    # Verify primary phone if we got one
    phone = prop.get("owner_phone")
    if phone:
        verification = verify_phone(phone)
        prop["phone_verified"] = verification.get("verified", False)
        prop["phone_safe_to_call"] = verification.get("safe_to_call", False)
        prop["phone_dnc"] = verification.get("dnc", False)
        prop["phone_tcpa_litigator"] = verification.get("tcpa_litigator", False)

    return prop
