# Changelog

All notable changes to `smart_fetch_orchestrator` are documented here.

## [3.1.1] — 2026-04-27

### SQLite TTL cache for ATTOM enricher

New module `smart_fetch/utils/api_cache.py` — generic SQLite-backed TTL cache, reusable for any API. Wired into the ATTOM enricher with per-endpoint TTLs:

| Endpoint family | TTL |
|---|---|
| Property identity (basicprofile, expandedprofile, detail, snapshot) | 90 days |
| Property detail (owner, mortgage) | 30 days |
| Building permits | 60 days |
| AVM (snapshot, detail, history) | 30 days |
| Assessment / tax history | 90 days |
| Sale + sale history | 60 days |

**Cache location:** `~/.smart_fetch/cache.db` (override via `SMART_FETCH_CACHE_DIR` env var). SQLite WAL mode, multi-process safe.

**API:**
- `from smart_fetch.utils.api_cache import get_cache` — process-wide singleton
- `cache.get(namespace, endpoint, params=...)` — None on miss, increments hit counter on hit
- `cache.put(namespace, endpoint, value, params=..., ttl_seconds=...)`
- `cache.invalidate(namespace, endpoint=None, params=None)` — bulk or specific
- `cache.purge_expired()` — manual cleanup
- `cache.stats(namespace=...)` — entry counts, hit counts per endpoint

**ATTOM-specific helpers:**
- `attom_enricher.cache_stats()` — pre-namespaced stats view
- `attom_enricher.invalidate_cache()` — drop all ATTOM cache entries

### Bug fix: ATTOM HTTP 400 quirk

ATTOM returns HTTP 400 with `status.msg: 'SuccessWithoutResult'` when a property has no data for an endpoint (e.g. AVM not available). Previously this surfaced as `_error` and was never cached — so every empty-data property kept hitting ATTOM forever. Now treated as a cacheable success-without-data response. Real errors (401/403/429/5xx) still surface as `_error`.

### Practical impact

For typical property pipelines (~100-500 enrichments/day with repeat lookups across daily runs), expected cache hit rate after warmup: 80-90%. Drops effective ATTOM API consumption from ~6,000-30,000/month to ~600-3,000/month — typically within trial quota.

Validated: cold call 1.24s → warm call 0.00s (cache hit).

## [3.1.0] — 2026-04-27

### ATTOM Property Data enricher

Added `smart_fetch/enrichers/attom_enricher.py` — wraps ATTOM's gold-standard property API.

**Trial-confirmed working endpoints:**
- `/property/expandedprofile` — primary call, covers identifier/address/assessment/sale/building/summary/location
- `/avm/snapshot` — AVM with confidence interval (low/high/score)
- `/saleshistory/detail` — full transaction history

**Field coverage** (all `attom_`-prefixed):
- IDs: `attom_id`, `attom_apn`, `attom_fips`, `attom_geoid`, `attom_geoidv4`
- Census: `attom_census_tract`, `attom_census_blockgroup`
- Distress flags: `attom_reo_flag`, `attom_absentee_owner`, `attom_quitclaim_flag`
- Valuation: `attom_avm`, `attom_avm_low`, `attom_avm_high`, `attom_avm_confidence`, `attom_market_value`
- Tax: `attom_assessed_total`, `attom_assessed_land`, `attom_assessed_imprv`, `attom_tax_amt`, `attom_tax_year`
- Building: `attom_sqft`, `attom_beds`, `attom_baths`, `attom_year_built`, `attom_condition`, `attom_construction_type`
- Lot: `attom_lot_size_sqft`, `attom_lot_size_acres`, `attom_zoning`
- Owner: `attom_owner_names`, `attom_owner_corporate`
- Mortgage: `attom_mortgage_amount`, `attom_mortgage_lender`, `attom_mortgage_date`, `attom_mortgage_term`
- Sale: `attom_last_sale_date`, `attom_last_sale_price`, `attom_last_sale_seller`, `attom_sale_history[]`

**Wiring in `orchestrator.fetch_property()`:**
- New flag `enrich_attom=False` (opt-in to preserve trial quota; ~2 API calls per property)
- Default OFF — explicit pass `enrich_attom=True` to use it
- Errors surface in `_attom_error` without breaking the fetch

**Validated end-to-end (Apr 27, 2026):**
- Test Denver address pulled `attom_avm=$697,734` (confidence 84, range $586k-$809k), `attom_reo_flag=True`, `attom_quitclaim_flag=True`, `attom_mortgage_amount=$510,000`, full last-sale + owner names + 2bd/1ba/1147sqft building details — all in one enricher call.

**Endpoints not on trial:** `/school/search` and `/salestrend/snapshot` returned 404. Skipped from the enricher; can be added if upgrading from trial.

### Config additions
- `ATTOM_API_KEY` env var (required), `ATTOM_BASE` constant
- `RATE_LIMITS["attom"]` set conservatively (60/min, 0.3s delay)
- `SOCRATA_APP_TOKEN` constant added for completeness (was env-only)

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
