"""Firecrawl-based fetcher — scrape, crawl, and URL discovery via Firecrawl API."""
import time
import requests
from smart_fetch.config import FIRECRAWL_API_KEY, FIRECRAWL_BASE, RATE_LIMITS

_RATE = RATE_LIMITS["firecrawl"]
_CRAWL_POLL_INTERVAL = 3   # seconds between polls
_CRAWL_MAX_POLLS = 60      # 3-minute total timeout


def _headers():
    return {
        "Authorization": f"Bearer {FIRECRAWL_API_KEY}",
        "Content-Type": "application/json",
    }


def _no_key():
    return {"success": False, "error": "FIRECRAWL_API_KEY not set"}


def fetch(url, formats=None, timeout=30, **kwargs):
    """Scrape a single URL via Firecrawl. Returns markdown + HTML.

    Returns:
        {"success": bool, "html": str, "markdown": str, "metadata": dict, "source": "firecrawl"}
    """
    if not FIRECRAWL_API_KEY:
        return _no_key()

    if formats is None:
        formats = ["markdown", "html"]

    payload = {"url": url, "formats": formats}

    try:
        resp = requests.post(
            f"{FIRECRAWL_BASE}/scrape",
            headers=_headers(),
            json=payload,
            timeout=timeout,
        )
        resp.raise_for_status()
        body = resp.json()
    except requests.exceptions.Timeout:
        return {"success": False, "error": f"request timed out after {timeout}s"}
    except requests.exceptions.HTTPError as exc:
        return {"success": False, "error": f"HTTP {exc.response.status_code}: {exc.response.text[:200]}"}
    except Exception as exc:
        return {"success": False, "error": str(exc)}
    finally:
        time.sleep(_RATE["delay_s"])

    if not body.get("success"):
        return {"success": False, "error": body.get("error", "unknown error from Firecrawl")}

    data = body.get("data", {})
    return {
        "success": True,
        "html": data.get("html", ""),
        "markdown": data.get("markdown", ""),
        "metadata": data.get("metadata", {}),
        "source": "firecrawl",
    }


def crawl(url, limit=10, depth=2, **kwargs):
    """Crawl multiple pages from a seed URL asynchronously with polling.

    Returns:
        {"success": bool, "records": [...], "pages_crawled": int, "source": "firecrawl_crawl"}
    """
    if not FIRECRAWL_API_KEY:
        return _no_key()

    payload = {
        "url": url,
        "limit": limit,
        "maxDepth": depth,
        "scrapeOptions": {"formats": ["markdown"]},
    }

    try:
        resp = requests.post(
            f"{FIRECRAWL_BASE}/crawl",
            headers=_headers(),
            json=payload,
            timeout=30,
        )
        resp.raise_for_status()
        body = resp.json()
    except requests.exceptions.Timeout:
        return {"success": False, "error": "crawl job submission timed out"}
    except requests.exceptions.HTTPError as exc:
        return {"success": False, "error": f"HTTP {exc.response.status_code}: {exc.response.text[:200]}"}
    except Exception as exc:
        return {"success": False, "error": str(exc)}
    finally:
        time.sleep(_RATE["delay_s"])

    if not body.get("success"):
        return {"success": False, "error": body.get("error", "crawl job submission failed")}

    job_id = body.get("id")
    if not job_id:
        return {"success": False, "error": "no job id returned by Firecrawl"}

    # Poll for results
    for _ in range(_CRAWL_MAX_POLLS):
        time.sleep(_CRAWL_POLL_INTERVAL)
        try:
            poll_resp = requests.get(
                f"{FIRECRAWL_BASE}/crawl/{job_id}",
                headers=_headers(),
                timeout=30,
            )
            poll_resp.raise_for_status()
            poll_body = poll_resp.json()
        except requests.exceptions.Timeout:
            return {"success": False, "error": "poll request timed out"}
        except requests.exceptions.HTTPError as exc:
            return {"success": False, "error": f"poll HTTP {exc.response.status_code}: {exc.response.text[:200]}"}
        except Exception as exc:
            return {"success": False, "error": str(exc)}

        status = poll_body.get("status", "")
        if status == "completed":
            records = poll_body.get("data", [])
            return {
                "success": True,
                "records": records,
                "pages_crawled": len(records),
                "source": "firecrawl_crawl",
            }
        if status in ("failed", "cancelled"):
            return {"success": False, "error": f"crawl job ended with status: {status}"}

    return {"success": False, "error": "crawl job timed out after 3 minutes"}


def map_urls(url, **kwargs):
    """Discover all URLs on a site without fetching content.

    Returns:
        {"success": bool, "links": [...], "source": "firecrawl_map"}
    """
    if not FIRECRAWL_API_KEY:
        return _no_key()

    payload = {"url": url}

    try:
        resp = requests.post(
            f"{FIRECRAWL_BASE}/map",
            headers=_headers(),
            json=payload,
            timeout=30,
        )
        resp.raise_for_status()
        body = resp.json()
    except requests.exceptions.Timeout:
        return {"success": False, "error": "map request timed out"}
    except requests.exceptions.HTTPError as exc:
        return {"success": False, "error": f"HTTP {exc.response.status_code}: {exc.response.text[:200]}"}
    except Exception as exc:
        return {"success": False, "error": str(exc)}
    finally:
        time.sleep(_RATE["delay_s"])

    if not body.get("success"):
        return {"success": False, "error": body.get("error", "unknown error from Firecrawl map")}

    return {
        "success": True,
        "links": body.get("links", []),
        "source": "firecrawl_map",
    }
