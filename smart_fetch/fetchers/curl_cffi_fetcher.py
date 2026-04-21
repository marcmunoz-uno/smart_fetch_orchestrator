"""Tier 1: curl_cffi with Chrome 131 TLS impersonation."""
import json, time
from smart_fetch.config import RATE_LIMITS

def fetch(url, impersonate="chrome131", timeout=20, **kwargs):
    """Fetch URL via curl_cffi. Best for sites without heavy JS rendering."""
    try:
        from curl_cffi import requests as cf
    except ImportError:
        return {"success": False, "error": "curl_cffi not installed"}

    try:
        session = cf.Session(impersonate=impersonate)
        resp = session.get(url, timeout=timeout)

        if resp.status_code == 403:
            return {"success": False, "error": f"blocked ({resp.status_code})", "status": 403}
        if resp.status_code != 200:
            return {"success": False, "error": f"HTTP {resp.status_code}", "status": resp.status_code}

        result = {"success": True, "html": resp.text, "status": 200, "url": url}

        # Try to parse as JSON
        try:
            result["json"] = resp.json()
        except:
            pass

        return result
    except Exception as e:
        return {"success": False, "error": str(e)}
    finally:
        time.sleep(RATE_LIMITS["curl_cffi"]["delay_s"])
