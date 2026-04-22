"""Audit county_portal_config.json — liveness-check every configured URL.

For each portal in the config, issues GET with a browser-like UA, records
(status, final_url, elapsed_ms, content_length). Flags:
  DEAD — status >= 400 or connection/timeout error
  REDIRECT — final_url differs from requested URL
  SLOW — >8s response
  OK — 2xx/3xx and landed where asked

Output: table to stdout + JSON at scripts/portal_audit_last_run.json.

Uses concurrent.futures for 16-way parallelism; bounded per-request timeout.
"""
import json
import sys
import time
import concurrent.futures as cf
from pathlib import Path
from urllib.parse import urlparse

import requests

CONFIG = Path.home() / "government-seized-scraper" / "county_portal_config.json"
OUT = Path(__file__).parent / "portal_audit_last_run.json"

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
HEADERS = {
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

TIMEOUT = 12
WORKERS = 16


def _collect_targets(cfg):
    rows = []
    markets = cfg.get("markets", cfg)
    for market, m in markets.items():
        for deal, p in (m.get("portals") or {}).items():
            url = p.get("url") or ""
            method = p.get("method") or ""
            if not url or method == "skip":
                continue
            rows.append({
                "market": market,
                "deal": deal,
                "method": method,
                "url": url,
            })
    return rows


def _check(row):
    url = row["url"]
    t0 = time.time()
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True)
        dt = int((time.time() - t0) * 1000)
        final = r.url
        redirected = urlparse(final).path != urlparse(url).path or urlparse(final).netloc != urlparse(url).netloc
        status = r.status_code
        length = len(r.content)
        if status >= 400:
            verdict = "DEAD"
        elif redirected:
            verdict = "REDIRECT"
        elif dt > 8000:
            verdict = "SLOW"
        else:
            verdict = "OK"
        return {**row, "status": status, "final_url": final, "elapsed_ms": dt,
                "bytes": length, "verdict": verdict, "error": None}
    except requests.exceptions.Timeout:
        return {**row, "status": 0, "final_url": None,
                "elapsed_ms": int((time.time() - t0) * 1000),
                "bytes": 0, "verdict": "DEAD", "error": "timeout"}
    except Exception as e:
        return {**row, "status": 0, "final_url": None,
                "elapsed_ms": int((time.time() - t0) * 1000),
                "bytes": 0, "verdict": "DEAD", "error": str(e)[:120]}


def main():
    if not CONFIG.exists():
        print(f"config not found: {CONFIG}", file=sys.stderr)
        sys.exit(1)

    cfg = json.loads(CONFIG.read_text())
    targets = _collect_targets(cfg)
    print(f"Auditing {len(targets)} portal URLs with {WORKERS} workers...\n")

    results = []
    with cf.ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for r in pool.map(_check, targets):
            results.append(r)

    # Sort: DEAD first, then REDIRECT, then SLOW, then OK
    order = {"DEAD": 0, "REDIRECT": 1, "SLOW": 2, "OK": 3}
    results.sort(key=lambda r: (order.get(r["verdict"], 9), r["market"], r["deal"]))

    print(f"{'verdict':<10} {'market':<20} {'deal':<14} {'status':<7} {'ms':<6} {'bytes':<8} url → final")
    print("-" * 140)
    for r in results:
        final_info = ""
        if r["verdict"] == "REDIRECT" and r.get("final_url"):
            final_info = f"→ {r['final_url'][:80]}"
        elif r.get("error"):
            final_info = f"ERR: {r['error'][:80]}"
        print(f"{r['verdict']:<10} {r['market']:<20} {r['deal']:<14} "
              f"{r['status']:<7} {r['elapsed_ms']:<6} {r['bytes']:<8} "
              f"{r['url'][:70]} {final_info}")

    # Summary
    counts = {}
    for r in results:
        counts[r["verdict"]] = counts.get(r["verdict"], 0) + 1
    print("\nSUMMARY:", ", ".join(f"{k}={v}" for k, v in sorted(counts.items(), key=lambda x: order.get(x[0], 9))))

    OUT.write_text(json.dumps(results, indent=2))
    print(f"\nFull results: {OUT}")


if __name__ == "__main__":
    main()
