"""Portal URL discovery via Firecrawl /search.

For every gap in county_portal_config.json (url='' or method='skip' without
a note saying "covered elsewhere"), runs a targeted Google/Bing search via
Firecrawl, ranks candidates by domain preference (.gov > known portal
patterns > other), liveness-checks the top 3, and writes a review-ready
JSON artifact. No config changes — human reviews the JSON and approves.

Rate: 3-worker pool, ~2s per call → ~5 min for ~80 gaps.

Usage:
    python3 scripts/discover_portal_urls.py
    python3 scripts/discover_portal_urls.py --limit 5      # smoke test
    python3 scripts/discover_portal_urls.py --market "Detroit MI"
"""
import os
import sys
import json
import time
import argparse
import concurrent.futures as cf
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import requests

# --- path + env bootstrap ---
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


CONFIG = Path.home() / "county-portal-scraper" / "county_portal_config.json"
SCRIPT_DIR = Path(__file__).resolve().parent

FIRECRAWL_SEARCH = "https://api.firecrawl.dev/v1/search"
WORKERS = 3

DEAL_QUERY_TAILS = {
    "foreclosure": "sheriff sale foreclosure auction public records",
    "probate": "probate court estate case search",
    "lis_pendens": "lis pendens recorder deed search",
    "land_bank": "land bank available properties for sale",
}

# Domain patterns we already know work, from existing successful portals.
# Matches are case-insensitive substring checks against the netloc.
KNOWN_PATTERNS = [
    "realforeclose.com",
    "sheriffsaleauction",
    "civilview.com",
    "courtsportal.",
    "clerkofcourts",
    "clerkscorner",
    "landbank",
    "probate.",
    "recorder.",
    "sheriff",
    "realforeclose",
    "realauction.com",
    "civicsource.com",
    "publicrec.",
    "taxsaleresources",
]

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")


def _is_intentional_skip(portal):
    note = (portal.get("note") or "").lower()
    return any(k in note for k in ("covered by", "statewide", "no dedicated", "n/a"))


def _find_gaps(cfg):
    gaps = []
    for market, m in cfg.get("markets", {}).items():
        for deal, p in (m.get("portals") or {}).items():
            if p.get("url") and p.get("method") != "skip":
                continue
            if _is_intentional_skip(p):
                continue
            gaps.append({
                "market": market,
                "county": m.get("county", ""),
                "state": m.get("state", ""),
                "deal": deal,
                "had_broken_url": p.get("_broken_url") or None,
            })
    return gaps


def _build_query(gap):
    tail = DEAL_QUERY_TAILS.get(gap["deal"], gap["deal"])
    return f'{gap["county"]} County {gap["state"]} {tail}'


def _score_url(url):
    """Higher score = better candidate. Rough heuristic."""
    try:
        netloc = urlparse(url).netloc.lower()
    except Exception:
        return 0
    score = 0
    # Official government domains
    if netloc.endswith(".gov"):
        score += 100
    if netloc.endswith(".us") or netloc.endswith(".state.us") or ".state." in netloc:
        score += 40
    # Known portal/platform patterns
    for pat in KNOWN_PATTERNS:
        if pat in netloc:
            score += 75
            break
    # Penalize generic aggregators / news / commercial realtor sites
    bad = ("zillow.com", "realtor.com", "redfin.com", "trulia.com",
           "wikipedia.org", "yelp.com", "youtube.com", "facebook.com",
           "foreclosure.com", "auction.com", "homes.com", "apartments.com")
    if any(b in netloc for b in bad):
        score -= 50
    return score


def _firecrawl_search(query, limit=5, timeout=30):
    key = os.environ.get("FIRECRAWL_API_KEY")
    if not key:
        return {"error": "FIRECRAWL_API_KEY missing", "results": []}
    try:
        r = requests.post(
            FIRECRAWL_SEARCH,
            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
            json={"query": query, "limit": limit},
            timeout=timeout,
        )
    except Exception as e:
        return {"error": f"request failed: {e}", "results": []}
    if r.status_code != 200:
        return {"error": f"HTTP {r.status_code}: {r.text[:200]}", "results": []}
    body = r.json()
    if not body.get("success"):
        return {"error": body.get("error", "search failed"), "results": []}
    return {"error": None, "results": body.get("data", [])}


def _liveness(url):
    try:
        r = requests.get(
            url, timeout=8, allow_redirects=True,
            headers={"User-Agent": UA, "Accept": "text/html,*/*;q=0.9"},
        )
        final = r.url
        redirected = urlparse(final).netloc != urlparse(url).netloc or \
                     urlparse(final).path.rstrip("/") != urlparse(url).path.rstrip("/")
        return {
            "status": r.status_code,
            "final_url": final if redirected else None,
            "alive": r.status_code < 400,
        }
    except Exception as e:
        return {"status": 0, "final_url": None, "alive": False, "error": str(e)[:120]}


def _process_gap(gap):
    query = _build_query(gap)
    search = _firecrawl_search(query, limit=5)
    candidates = []
    for r in search.get("results", []):
        url = (r.get("url") or "").strip()
        if not url:
            continue
        score = _score_url(url)
        candidates.append({
            "url": url,
            "title": (r.get("title") or "")[:120],
            "description": (r.get("description") or "")[:200],
            "score": score,
        })
    candidates.sort(key=lambda c: c["score"], reverse=True)
    top = candidates[:3]
    for c in top:
        c.update(_liveness(c["url"]))
        time.sleep(0.2)
    return {
        **gap,
        "query": query,
        "search_error": search.get("error"),
        "candidates": top,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, help="process only first N gaps (smoke test)")
    ap.add_argument("--market", help="only process this market (e.g. 'Detroit MI')")
    args = ap.parse_args()

    _load_env()
    if not os.environ.get("FIRECRAWL_API_KEY"):
        print("FIRECRAWL_API_KEY not set. Check ~/.openclaw/.env", file=sys.stderr)
        sys.exit(1)

    cfg = json.loads(CONFIG.read_text())
    gaps = _find_gaps(cfg)
    if args.market:
        gaps = [g for g in gaps if g["market"] == args.market]
    if args.limit:
        gaps = gaps[: args.limit]

    print(f"Processing {len(gaps)} portal gaps with {WORKERS} workers...")
    print(f"Firecrawl rate limit: ~30 searches/min. Estimated time: {len(gaps) * 2 // WORKERS}s\n")

    t0 = time.time()
    results = []
    with cf.ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(_process_gap, g): g for g in gaps}
        for i, fut in enumerate(cf.as_completed(futures), 1):
            r = fut.result()
            results.append(r)
            top = r["candidates"][0] if r["candidates"] else None
            mark = "OK" if top and top.get("alive") else ("SOFT" if top else "--")
            top_url = (top or {}).get("url", "(no candidates)")[:60]
            top_score = (top or {}).get("score", 0)
            print(f"[{i:3}/{len(gaps)}] [{mark:4}] {r['market']:<18} {r['deal']:<14} "
                  f"s={top_score:<4} {top_url}")

    total = time.time() - t0

    # Sort: by market, then deal, for readable output
    results.sort(key=lambda r: (r["market"], r["deal"]))

    # Summary counts
    total_gaps = len(results)
    with_live = sum(1 for r in results if r["candidates"] and any(c.get("alive") for c in r["candidates"]))
    with_gov = sum(1 for r in results if r["candidates"] and any(c["score"] >= 100 for c in r["candidates"]))
    no_candidates = sum(1 for r in results if not r["candidates"])

    print(f"\n{'='*70}")
    print(f"FINISHED in {total:.0f}s")
    print(f"  gaps processed:       {total_gaps}")
    print(f"  at least 1 live hit:  {with_live}")
    print(f"  .gov hit in top 3:    {with_gov}")
    print(f"  zero candidates:      {no_candidates}")

    date = datetime.utcnow().strftime("%Y-%m-%d")
    out = SCRIPT_DIR / f"portal_candidates_{date}.json"
    out.write_text(json.dumps(results, indent=2))
    print(f"\nReview artifact: {out}")
    print(f"Open it, pick URLs, and I'll wire them into county_portal_config.json.")


if __name__ == "__main__":
    main()
