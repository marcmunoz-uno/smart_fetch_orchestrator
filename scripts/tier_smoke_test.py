"""Tier-by-tier smoke test for smart_fetch_orchestrator.

Runs a small URL matrix against forced-tier fetchers (curl_cffi, firecrawl,
browserbase) plus the auto-routed fetch_url() chain. Logs per-call: tier,
success, latency, bytes, first 80 chars of body, error.

Usage:
    python3 scripts/tier_smoke_test.py

Reads secrets from ~/.openclaw/.env into os.environ before importing
smart_fetch (its config.py binds keys at import time).
"""
import os
import sys
import time
import json
from pathlib import Path

# Make `smart_fetch` importable without `pip install -e .` (memory claimed editable
# install but the package isn't in site-packages on this machine).
_PKG_ROOT = Path(__file__).resolve().parent.parent
if str(_PKG_ROOT) not in sys.path:
    sys.path.insert(0, str(_PKG_ROOT))


def _load_env():
    env_path = Path.home() / ".openclaw" / ".env"
    if not env_path.exists():
        print(f"WARN: {env_path} not found — API-key tiers will skip", file=sys.stderr)
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


_load_env()

from smart_fetch.fetchers import (  # noqa: E402
    curl_cffi_fetcher,
    firecrawl_fetcher,
    browserbase_fetcher,
)
from smart_fetch.orchestrator import fetch_url  # noqa: E402


# (label, url, forced_tiers_to_try)
TARGETS = [
    ("zillow_search",  "https://www.zillow.com/homes/for_sale/Tampa-FL_rb/",
        ["curl_cffi", "firecrawl", "browserbase"]),
    ("bid4assets",     "https://www.bid4assets.com/",
        ["curl_cffi", "firecrawl", "browserbase"]),
    ("hillsclerk",     "https://www.hillsclerk.com/Official-Records/Foreclosures",
        ["curl_cffi", "firecrawl", "browserbase"]),
    ("detroit_landbank", "https://buildingdetroit.org/available-properties",
        ["curl_cffi", "firecrawl", "browserbase"]),
    ("redfin_search",  "https://www.redfin.com/city/30772/FL/Tampa",
        ["curl_cffi", "firecrawl"]),
    ("control",        "https://example.com",
        ["curl_cffi", "firecrawl"]),
]

FETCHERS = {
    "curl_cffi":   curl_cffi_fetcher.fetch,
    "firecrawl":   firecrawl_fetcher.fetch,
    "browserbase": browserbase_fetcher.fetch,
}


def _body(result):
    return result.get("html") or result.get("markdown") or ""


def _preview(s, n=80):
    return (s or "").replace("\n", " ").replace("\r", " ")[:n]


def run_forced():
    print(f"\n{'='*110}")
    print("FORCED-TIER RESULTS")
    print("=" * 110)
    print(f"{'target':<20} {'tier':<13} {'ok':<4} {'lat_s':<7} {'bytes':<8} preview / error")
    print("-" * 110)
    rows = []
    for label, url, tiers in TARGETS:
        for tier in tiers:
            fn = FETCHERS[tier]
            t0 = time.time()
            try:
                r = fn(url)
            except Exception as e:
                r = {"success": False, "error": f"EXC: {e}"}
            dt = time.time() - t0
            body = _body(r)
            ok = "Y" if r.get("success") else "N"
            disp = _preview(body, 80) if r.get("success") else f"ERR: {r.get('error','?')[:80]}"
            print(f"{label:<20} {tier:<13} {ok:<4} {dt:<7.2f} {len(body):<8} {disp}")
            rows.append({
                "target": label, "url": url, "tier": tier,
                "success": bool(r.get("success")),
                "latency_s": round(dt, 2),
                "bytes": len(body),
                "error": r.get("error"),
                "preview": _preview(body, 200),
            })
    return rows


def run_auto():
    print(f"\n{'='*110}")
    print("AUTO-ROUTED fetch_url() — which tier wins?")
    print("=" * 110)
    print(f"{'target':<20} {'winner':<13} {'ok':<4} {'lat_s':<7} {'bytes':<8} attempts")
    print("-" * 110)
    rows = []
    for label, url, _ in TARGETS:
        t0 = time.time()
        try:
            r = fetch_url(url)
        except Exception as e:
            r = {"success": False, "error": f"EXC: {e}", "attempts": []}
        dt = time.time() - t0
        body = _body(r)
        ok = "Y" if r.get("success") else "N"
        winner = r.get("fetcher_used") or "-"
        attempts = r.get("attempts", [])
        att_str = " > ".join(
            f"{a.get('fetcher')}({'ok' if a.get('success') else 'x'})"
            for a in attempts
        )
        print(f"{label:<20} {winner:<13} {ok:<4} {dt:<7.2f} {len(body):<8} {att_str}")
        rows.append({
            "target": label, "url": url, "winner": winner,
            "success": bool(r.get("success")),
            "latency_s": round(dt, 2),
            "bytes": len(body),
            "attempts": attempts,
        })
    return rows


def main():
    print("smart_fetch tier smoke test")
    print(f"FIRECRAWL_API_KEY: {'set' if os.environ.get('FIRECRAWL_API_KEY') else 'MISSING'}")
    print(f"BROWSERBASE_API_KEY: {'set' if os.environ.get('BROWSERBASE_API_KEY') else 'MISSING'}")
    print(f"BROWSERBASE_PROJECT_ID: {'set' if os.environ.get('BROWSERBASE_PROJECT_ID') else 'MISSING'}")

    forced = run_forced()
    auto = run_auto()

    out = Path(__file__).parent / "tier_smoke_last_run.json"
    out.write_text(json.dumps({"forced": forced, "auto": auto}, indent=2))
    print(f"\nFull results: {out}")


if __name__ == "__main__":
    main()
