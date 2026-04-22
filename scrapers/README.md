# Zillow Scrapers

Production Zillow scrapers that feed the Tranchi upload pipeline. Moved
into `smart_fetch_orchestrator/scrapers/` on 2026-04-22; previously
lived at `~/.openclaw/workspace/zillow_monitor/`.

- `zillow_for_sale.py` — for-sale listings monitor. Runs nightly at 6am
  via `Resilient Zillow Scraper (31 Markets)` cron; writes qualifying
  cash-flow properties to `/tmp/for_sale_properties.json`. Tier-1 uses
  `smart_fetch.orchestrator.fetch_market` (Firecrawl-bypass of PX);
  falls back to `curl_cffi` and BrightData-enrichment-of-cache if
  Tier-1 is empty.
- `zillow_monitor.py` — rental listings monitor for DSCR lead-finding
  (original purpose; see rental lead-scoring strategy below).
- `seed_new_markets.py` — market-bounds bootstrapping helper.

The Python venv used by the cron still lives at
`~/.openclaw/workspace/zillow_monitor/venv/`. If you recreate it inside
this repo, update the cron command in
`~/.openclaw/cron/jobs.json` accordingly.

---

## DSCR Lead-Finding Strategy (zillow_monitor.py)

Find overleveraged landlords who might need DSCR refinancing by monitoring Zillow rental listings.

## Strategy

DSCR (Debt Service Coverage Ratio) loans are evaluated based on rental income vs. mortgage payment. Overleveraged landlords—those whose rental income barely covers (or doesn't cover) their mortgage—are prime candidates for DSCR refinancing.

### Signals of an Overleveraged Landlord

| Signal | Why It Matters |
|--------|---------------|
| **Low rent-to-value ratio** | If annual rent < 7% of property value, owner likely overpaid or has high leverage |
| **Asking below rent Zestimate** | Pricing below market = desperate to fill vacancy |
| **30+ days on market** | Struggling to find tenants = cash flow pressure |
| **Price cuts** | Reducing rent = willing to lose money to avoid vacancy |
| **Multiple listings same owner** | Portfolio stress if several units vacant |

### Scoring (0-10)

- **7-10**: High likelihood of needing refi (multiple distress signals)
- **4-6**: Moderate—worth reaching out
- **1-3**: Low signals, but may still be receptive

## Setup

```bash
cd zillow_monitor
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Usage

```bash
# All default markets
python3 zillow_monitor.py

# Specific markets
python3 zillow_monitor.py --markets "Indianapolis IN" "Miami FL"

# Single market test
python3 zillow_monitor.py --markets "Indianapolis IN"
```

## Output

Leads saved to:
- `leads/YYYY-MM-DD.json` — dated lead files
- `/tmp/zillow_leads.json` — latest run

Each lead:
```json
{
  "zpid": "12345",
  "address": "123 Main St",
  "city": "Indianapolis",
  "state": "IN",
  "asking_rent": 1500,
  "estimated_value": 250000,
  "days_on_market": 45,
  "price_cuts": 1,
  "dscr_score": 8.5,
  "reason": "Very low rent-to-value (5.2% annual yield); Long DOM (45 days); Price cut detected",
  "zillow_url": "https://www.zillow.com/homedetails/..."
}
```

## Target Markets

Miami FL, Houston TX, Dallas TX, Atlanta GA, Phoenix AZ, Indianapolis IN, Kansas City MO, Cleveland OH

## Deduplication

Each run checks against previous lead files in `leads/` to avoid surfacing the same property twice.

## Notes

- Zillow may rate-limit or block requests. The tool uses browser-like headers and delays between requests.
- Zestimate values may not be available for all properties.
- Run daily for best results—new listings and price cuts appear regularly.
