"""Run the smart_fetch orchestrator against every configured county portal.

Unlike audit_portal_urls.py (which does a plain requests.get), this uses
fetch_url() so the 7-tier chain is exercised per portal. Useful for seeing
whether firecrawl/browserbase/playwright can recover portals that blocked
the plain GET.

Concurrency is LOW (4 workers) because browserbase and firecrawl have rate
limits and per-call costs.

Output: table to stdout + JSON at scripts/portal_fetch_sweep_last_run.json.
"""
import os
import sys
import json
import time
import concurrent.futures as cf
from pathlib import Path

# --- path + env bootstrap (same pattern as tier_smoke_test.py) ---
_PKG_ROOT = Path(__file__).resolve().parent.parent
if str(_PKG_ROOT) not in sys.path:
    sys.path.insert(0, str(_PKG_ROOT))


def _load_env():
    env_path = Path.home() / ".openclaw" / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


_load_env()

from smart_fetch.orchestrator import fetch_url  # noqa: E402

CONFIG = Path.home() / "county-portal-scraper" / "county_portal_config.json"
OUT = Path(__file__).parent / "portal_fetch_sweep_last_run.json"
WORKERS = 4


def _collect(cfg):
    rows = []
    for market, m in cfg.get("markets", cfg).items():
        for deal, p in (m.get("portals") or {}).items():
            if not p.get("url") or p.get("method") == "skip":
                continue
            rows.append({
                "market": market, "deal": deal,
                "method": p.get("method"),
                "url": p["url"],
            })
    return rows


def _attempt_summary(attempts):
    if not attempts:
        return ""
    return " > ".join(
        f"{a.get('fetcher')}({'ok' if a.get('success') else 'x'})"
        for a in attempts
    )


def _run(row):
    t0 = time.time()
    try:
        r = fetch_url(row["url"])
    except Exception as e:
        r = {"success": False, "error": f"EXC: {e}", "attempts": []}
    dt = time.time() - t0
    body = r.get("html") or r.get("markdown") or ""
    return {
        **row,
        "success": bool(r.get("success")),
        "winner": r.get("fetcher_used") or "-",
        "latency_s": round(dt, 2),
        "bytes": len(body),
        "attempts": _attempt_summary(r.get("attempts", [])),
        "error": r.get("error"),
    }


def main():
    cfg = json.loads(CONFIG.read_text())
    targets = _collect(cfg)
    print(f"Sweeping {len(targets)} portals through fetch_url() with {WORKERS} workers...")
    print(f"FIRECRAWL_API_KEY: {'set' if os.environ.get('FIRECRAWL_API_KEY') else 'MISSING'}")
    print(f"BROWSERBASE_API_KEY: {'set' if os.environ.get('BROWSERBASE_API_KEY') else 'MISSING'}\n")

    results = []
    t0 = time.time()
    with cf.ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for r in pool.map(_run, targets):
            results.append(r)
            ok = "Y" if r["success"] else "N"
            print(f"[{ok}] {r['market']:<20} {r['deal']:<14} {r['winner']:<13} "
                  f"{r['latency_s']:<6}s {r['bytes']:<8}b  {r['attempts']}")

    total = time.time() - t0

    # Sort: failures first
    results.sort(key=lambda r: (r["success"], r["market"], r["deal"]))
    wins = sum(1 for r in results if r["success"])
    by_winner = {}
    for r in results:
        if r["success"]:
            by_winner[r["winner"]] = by_winner.get(r["winner"], 0) + 1

    print(f"\n{'='*80}")
    print(f"RESULT: {wins}/{len(results)} portals fetched successfully in {total:.1f}s")
    print(f"Winning tier distribution: {by_winner}")
    print(f"\nFailures:")
    for r in results:
        if not r["success"]:
            print(f"  {r['market']:<20} {r['deal']:<14} {r['attempts']}  ERR: {r.get('error','?')[:60]}")

    OUT.write_text(json.dumps(results, indent=2))
    print(f"\nFull results: {OUT}")


if __name__ == "__main__":
    main()
