# Changelog

All notable changes to `smart_fetch_orchestrator` are documented here.

## [3.0.0] — 2026-04-27

### Three new direct-API fetchers + schema-driven AI extraction

Adds API-first data sources that bypass HTML scraping for portals that publish data directly. Pulls structured JSON instead of regex-parsing markdown.

**New fetchers (`smart_fetch/fetchers/`):**

- `socrata_fetcher.py` — SoQL client for Socrata Open Data Portals. Auto-paginates with `$limit`/`$offset`, supports `$where`/`$select`/`$order`. Optional `SOCRATA_APP_TOKEN` env var lifts rate limit to 1000 req/hour. Includes `normalize_records(field_map=...)` to flatten Socrata fields into the spider's standard property record shape.
- `firecrawl_extract.py` — schema-based AI extraction via Firecrawl `/v1/extract`. Async API: submits, polls until completed, returns structured JSON matching the schema. Looks up platform schemas from `extract_schemas.py` by name.
- `arcgis_fetcher.py` — direct ArcGIS REST `/MapServer/<n>/query` client. Auto-paginates via `resultOffset` + `exceededTransferLimit`. Includes `normalize_features()` to flatten Esri attributes into the standard shape.

**New module (`smart_fetch/extract_schemas.py`):**

Per-platform JSON schemas + extraction prompts for Firecrawl `/extract`. Seven platforms registered:

- `realauction` — `*.realforeclose.com`, `*.sheriffsaleauction.ohio.gov`
- `epropertyplus` — Atlanta, Kansas City, Lansing land banks
- `bid4assets` — Philadelphia foreclosures, Wayne County tax sales
- `civilview` — Louisiana parish sheriff sales
- `gsccca_lien` — Georgia statewide lien index (foreclosure deeds + lis pendens)
- `oscn_dockets` — Oklahoma State Court Network probate/foreclosure
- `civicengage_table` — generic CivicEngage CMS gov sites (catch-all)

All schemas produce a uniform `{listings: [{address, case_number, parcel_id, opening_bid, sale_date, ...}]}` shape so downstream code is platform-agnostic.

### Validated end-to-end

- NOLA Sheriff Sales (Socrata): 1,771 records in ~3s
- Chicago city-owned land (Socrata): 1,897 records
- Baton Rouge adjudicated property (Socrata): 4,527 records
- NYC tax lien sale (Socrata): 4,588 records
- Orlando vacant lots (Socrata): 61 records
- Firecrawl extract submit/poll/return cycle: confirmed working against Lucas County (returned valid empty `listings[]` for an auction-calendar landing page; correct behavior)

### Rollback

Pre-v3 baseline at `v2.0.0` (commit `ff76dbb`). Roll back with:

```bash
git checkout v2.0.0
pip install -e .
```

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
