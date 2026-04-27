# Changelog

All notable changes to `smart_fetch_orchestrator` are documented here.

## [2.0.0] — 2026-04-27

### Validator: split market-value vs tax-assessed price sources

The 50% price-divergence flag previously compared Zillow listing price, HouseCanary AVM, and BatchData tax-assessed under a single threshold. In Florida, the Save Our Homes cap keeps tax-assessed at 40–60% of market value, so every validated FL property carried a spurious `price_divergence_Xpct` flag. Same problem (smaller magnitude) applies in any state with a homestead cap or where assessment lags market.

**Changes (`smart_fetch/validator.py`):**

- Split price collection: `market_prices` (Zillow listing + HC AVM) vs `tax_assessed` (BatchData)
- Divergence check now runs only against market-value sources — no more false positives from cap-suppressed tax assessments
- Tax-assessed is still tracked in `sources_used` and used as a fallback for `best_price` when no market source is available
- New sanity flag: `tax_far_above_market` fires when tax-assessed is more than 2× any market estimate (signals wrong property match, not the SOH cap)
- `prices` dict still populated (with tax included) for backward compatibility with downstream code that reads it (e.g. `for_sale_rental_detector.py`)

### Rollback

Pre-change baseline tagged as `v1.0.0` (commit `a19c334`). To roll back:

```bash
git checkout v1.0.0
pip install -e .
```

## [1.0.0] — 2026-04-23 (retroactive tag)

Baseline before the v2.0 validator split. Tagged retroactively at commit `a19c334` as a rollback point. Includes:

- 7-tier fetcher chain: curl_cffi → BrightData → Firecrawl → Browserbase → Playwright → Cloudflare /crawl → requests
- 3 enrichers: HouseCanary (AVM/NOD/flood/LTV), BrightData (Zillow live), BatchData (skip trace)
- Cross-source validator with single price-divergence threshold across all sources
- Wired into 6 scraper files (zillow_for_sale, foreclosure_3city, auction_markets, zillow_to_mcp_bridge, daily_tranchi_pipeline, motivated_seller_pipeline)

## [0.1.0] — 2026-04-21

Initial release — orchestrator scaffolding, fetcher tiers 1–5, basic validator.
