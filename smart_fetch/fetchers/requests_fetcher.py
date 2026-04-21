"""Tier 5: Plain requests — simplest fallback for unprotected APIs and sites."""
import json, time, requests as req
from smart_fetch.config import RATE_LIMITS

def fetch(url, headers=None, timeout=15, **kwargs):
    """Basic HTTP GET via requests. No anti-bot bypassing."""
    try:
        default_headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        }
        if headers:
            default_headers.update(headers)

        resp = req.get(url, headers=default_headers, timeout=timeout)

        if resp.status_code != 200:
            return {"success": False, "error": f"HTTP {resp.status_code}", "status": resp.status_code}

        result = {"success": True, "html": resp.text, "status": 200, "source": "requests"}
        try:
            result["json"] = resp.json()
        except:
            pass

        return result
    except Exception as e:
        return {"success": False, "error": str(e)}
