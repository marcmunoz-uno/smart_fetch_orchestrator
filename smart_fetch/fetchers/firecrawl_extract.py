"""Firecrawl /extract — schema-based AI extraction for HTML portals.

Async API: POST /v1/extract returns a job ID; poll GET /v1/extract/{id}
until status='completed'. The structured `data` field comes back matching
the JSON schema we passed in.

Used as `method: firecrawl_extract` in the county portal spider config.
The platform schema is selected by `platform` key on the portal config —
see smart_fetch/extract_schemas.py.
"""

import time
import requests
from typing import Optional

from smart_fetch.config import FIRECRAWL_API_KEY, FIRECRAWL_BASE, RATE_LIMITS
from smart_fetch.extract_schemas import get_schema

_RATE = RATE_LIMITS["firecrawl"]
_POLL_INTERVAL = 4
_MAX_POLLS = 45  # ~3 min total


def _headers():
    return {
        "Authorization": f"Bearer {FIRECRAWL_API_KEY}",
        "Content-Type": "application/json",
    }


def fetch(
    url: str,
    *,
    platform: Optional[str] = None,
    schema: Optional[dict] = None,
    prompt: Optional[str] = None,
    enable_web_search: bool = False,
    timeout: int = 30,
    **_,
) -> dict:
    """Run Firecrawl /extract on a URL using a platform schema or inline schema.

    Provide one of:
      - platform: looked up in extract_schemas.PLATFORM_SCHEMAS
      - schema + prompt: inline JSON schema and extraction prompt

    Returns:
      {"success": bool, "data": dict, "records": list, "source": "firecrawl_extract",
       "platform": str, "error": str?}

    `records` is the most-likely list field in `data` (e.g. data.listings) flattened
    so callers don't need to know the schema shape.
    """
    if not FIRECRAWL_API_KEY:
        return {"success": False, "error": "FIRECRAWL_API_KEY not set"}

    if platform:
        spec = get_schema(platform)
        if not spec:
            return {"success": False, "error": f"unknown platform schema: {platform}"}
        schema = schema or spec["schema"]
        prompt = prompt or spec["prompt"]

    if not schema and not prompt:
        return {"success": False, "error": "must provide platform OR schema/prompt"}

    payload = {
        "urls": [url],
        "ignoreInvalidURLs": True,
    }
    if schema:
        payload["schema"] = schema
    if prompt:
        payload["prompt"] = prompt
    if enable_web_search:
        payload["enableWebSearch"] = True

    try:
        resp = requests.post(
            f"{FIRECRAWL_BASE}/extract",
            headers=_headers(),
            json=payload,
            timeout=timeout,
        )
        body = resp.json()
    except requests.exceptions.Timeout:
        return {"success": False, "error": f"submit timed out after {timeout}s", "platform": platform}
    except Exception as exc:
        return {"success": False, "error": f"submit failed: {exc}", "platform": platform}

    if resp.status_code >= 400 or not body.get("success"):
        return {
            "success": False,
            "error": f"submit error: {body.get('error', resp.text[:200])}",
            "platform": platform,
        }

    job_id = body.get("id")
    if not job_id:
        # Synchronous response variant — data already present
        data = body.get("data") or {}
        return _shape_response(data, platform=platform, url=url)

    # Poll for completion
    for _ in range(_MAX_POLLS):
        time.sleep(_POLL_INTERVAL)
        try:
            poll = requests.get(
                f"{FIRECRAWL_BASE}/extract/{job_id}",
                headers=_headers(),
                timeout=timeout,
            )
            poll_body = poll.json()
        except Exception as exc:
            return {"success": False, "error": f"poll failed: {exc}", "job_id": job_id, "platform": platform}

        status = (poll_body.get("status") or "").lower()
        if status == "completed":
            data = poll_body.get("data") or {}
            return _shape_response(data, platform=platform, url=url, job_id=job_id)
        if status in ("failed", "cancelled", "error"):
            return {
                "success": False,
                "error": f"job {status}: {poll_body.get('error', 'unknown')}",
                "job_id": job_id,
                "platform": platform,
            }

    time.sleep(_RATE["delay_s"])
    return {"success": False, "error": "extract poll timeout", "job_id": job_id, "platform": platform}


def _shape_response(data: dict, *, platform: Optional[str], url: str, job_id: Optional[str] = None) -> dict:
    """Find the listings array in the response data and return it as `records`."""
    records: list = []
    if isinstance(data, dict):
        # First top-level array key wins (schemas are designed to nest results in one)
        for key in ("listings", "records", "results", "items", "properties"):
            if isinstance(data.get(key), list):
                records = data[key]
                break
        # Fallback: any list
        if not records:
            for v in data.values():
                if isinstance(v, list):
                    records = v
                    break
    elif isinstance(data, list):
        records = data

    return {
        "success": True,
        "data": data,
        "records": records,
        "source": "firecrawl_extract",
        "platform": platform,
        "url": url,
        "job_id": job_id,
    }
