"""Socrata SODA API fetcher — query open-data datasets directly via SoQL.

Each Socrata dataset is addressable as `https://{domain}/resource/{4x4}.json`.
Supports SoQL params: $select, $where, $order, $limit, $offset.
App token (optional) lifts rate limit to 1000 req/hour; without it, requests
share a throttled pool. Token is read from SOCRATA_APP_TOKEN env var.

Used as `method: socrata_query` in the county portal spider config.
"""

import os
import time
import requests
from typing import Optional

SOCRATA_APP_TOKEN = os.environ.get("SOCRATA_APP_TOKEN", "")
PAGE_SIZE = 50_000  # SODA hard cap per response


def _headers():
    h = {"Accept": "application/json"}
    if SOCRATA_APP_TOKEN:
        h["X-App-Token"] = SOCRATA_APP_TOKEN
    return h


def fetch(
    url: Optional[str] = None,
    *,
    domain: Optional[str] = None,
    dataset_id: Optional[str] = None,
    where: Optional[str] = None,
    select: Optional[str] = None,
    order: Optional[str] = None,
    limit: int = PAGE_SIZE,
    max_records: int = 200_000,
    timeout: int = 30,
    **_,
) -> dict:
    """Query a Socrata dataset and return all records (auto-paginated).

    Two call styles:
      fetch(url="https://data.cityofchicago.org/resource/22u3-xenr.json", where="...")
      fetch(domain="data.cityofchicago.org", dataset_id="22u3-xenr", where="...")

    Returns:
      {"success": bool, "records": [...], "row_count": int, "source": "socrata",
       "url": str, "error": str?}
    """
    if url is None:
        if not domain or not dataset_id:
            return {"success": False, "error": "must provide url OR (domain + dataset_id)"}
        url = f"https://{domain}/resource/{dataset_id}.json"

    all_rows: list = []
    offset = 0
    page_size = min(limit, PAGE_SIZE)

    while True:
        params = {"$limit": page_size, "$offset": offset}
        if where:  params["$where"]  = where
        if select: params["$select"] = select
        if order:  params["$order"]  = order

        try:
            resp = requests.get(url, headers=_headers(), params=params, timeout=timeout)
        except requests.exceptions.Timeout:
            return {
                "success": False,
                "error": f"socrata timed out after {timeout}s",
                "url": url,
                "records": all_rows,
            }
        except Exception as exc:
            return {
                "success": False,
                "error": f"socrata fetch error: {exc}",
                "url": url,
                "records": all_rows,
            }

        if resp.status_code == 429:
            # Socrata throttled — back off and retry once
            time.sleep(5)
            try:
                resp = requests.get(url, headers=_headers(), params=params, timeout=timeout)
            except Exception as exc:
                return {"success": False, "error": f"throttled, retry failed: {exc}", "url": url}

        if resp.status_code != 200:
            return {
                "success": False,
                "error": f"socrata HTTP {resp.status_code}: {resp.text[:200]}",
                "url": url,
                "records": all_rows,
            }

        try:
            page = resp.json()
        except Exception as exc:
            return {"success": False, "error": f"socrata JSON parse: {exc}", "url": url}

        if not isinstance(page, list):
            return {
                "success": False,
                "error": f"socrata returned non-list: {str(page)[:200]}",
                "url": url,
            }

        all_rows.extend(page)

        if len(page) < page_size or len(all_rows) >= max_records:
            break
        offset += page_size

        # Pace ourselves — even with token, polite is good
        time.sleep(0.2)

    return {
        "success": True,
        "records": all_rows[:max_records],
        "row_count": min(len(all_rows), max_records),
        "source": "socrata",
        "url": url,
    }


def normalize_records(
    records: list,
    *,
    deal_type: str = "",
    field_map: Optional[dict] = None,
) -> list:
    """Flatten Socrata records into the spider's standard property dict shape.

    field_map (optional) explicitly maps Socrata field names → standard keys:
      {"property_address": "address", "tax_amount": "price", "case_number": "case_number"}

    Without field_map, falls back to heuristic key matching. Always preserves
    raw fields under `_raw`.
    """
    out = []
    for row in records:
        if not isinstance(row, dict):
            continue
        rec: dict = {"_raw": {k: v for k, v in row.items() if not isinstance(v, (dict, list))}}
        if deal_type:
            rec["deal_type"] = deal_type

        if field_map:
            for src_key, dst_key in field_map.items():
                if row.get(src_key) is not None:
                    rec[dst_key] = row[src_key]
        else:
            # Heuristic
            for k, v in row.items():
                if v is None or v == "":
                    continue
                kl = k.lower()
                if "address" in kl and "address" not in rec:
                    rec["address"] = str(v)
                elif kl in ("city", "municipality") and "city" not in rec:
                    rec["city"] = str(v)
                elif kl in ("state", "state_code"):
                    rec["state"] = str(v)
                elif kl in ("zip", "zip_code", "zipcode", "postal_code"):
                    rec["zip_code"] = str(v)
                elif "price" in kl or "amount" in kl or "value" in kl:
                    if "price" not in rec:
                        try:
                            rec["price"] = str(int(float(str(v).replace("$", "").replace(",", ""))))
                        except (ValueError, TypeError):
                            pass
                elif "case" in kl or "docket" in kl or "parcel" in kl or "pin" in kl:
                    if "case_number" not in rec:
                        rec["case_number"] = str(v)
                elif "date" in kl and "filed_date" not in rec:
                    rec["filed_date"] = str(v)

        # Skip rows with no usable data
        if rec.get("address") or rec.get("case_number"):
            out.append(rec)

    return out
