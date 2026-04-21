"""Tier 4: Cloudflare Browser Rendering /crawl — multi-page spidering for gov/county sites."""
import json, time, requests as req
from smart_fetch.config import CLOUDFLARE_API_KEY, CLOUDFLARE_ACCOUNT_ID, CLOUDFLARE_EMAIL, RATE_LIMITS

CF_BASE = f"https://api.cloudflare.com/client/v4/accounts/{CLOUDFLARE_ACCOUNT_ID}/browser-rendering/crawl"
CF_HEADERS = {
    "X-Auth-Key": CLOUDFLARE_API_KEY,
    "X-Auth-Email": CLOUDFLARE_EMAIL,
    "Content-Type": "application/json",
}

def fetch(url, limit=10, depth=2, render=True, formats=None, poll_interval=3, max_polls=40, **kwargs):
    """Submit a crawl job and poll for results. Returns rendered markdown/HTML."""
    if formats is None:
        formats = ["markdown"]

    try:
        # Submit job
        resp = req.post(CF_BASE, headers=CF_HEADERS, json={
            "url": url,
            "limit": limit,
            "depth": depth,
            "render": render,
            "formats": formats,
        }, timeout=15)

        if resp.status_code != 200:
            body = resp.text[:200]
            return {"success": False, "error": f"submit failed ({resp.status_code}): {body}"}

        data = resp.json()
        if not data.get("success"):
            return {"success": False, "error": f"CF error: {data}"}

        job_id = data["result"]

        # Poll for results
        for _ in range(max_polls):
            time.sleep(poll_interval)
            poll = req.get(f"{CF_BASE}/{job_id}", headers=CF_HEADERS, timeout=15)

            if poll.status_code != 200:
                continue

            result = poll.json().get("result", {})
            status = result.get("status")

            if status == "completed":
                records = result.get("records", [])
                return {
                    "success": True,
                    "records": records,
                    "pages_crawled": len(records),
                    "source": "cloudflare_crawl",
                    # Flatten first record for convenience
                    "html": records[0].get("html", "") if records else "",
                    "markdown": records[0].get("markdown", "") if records else "",
                }
            elif status == "error":
                return {"success": False, "error": f"crawl failed: {result.get('error', 'unknown')}"}

        return {"success": False, "error": "poll timeout"}
    except Exception as e:
        return {"success": False, "error": str(e)}
