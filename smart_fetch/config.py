import os

# BrightData MCP
BRIGHTDATA_TOKEN = os.environ.get("BRIGHTDATA_TOKEN", "")
BRIGHTDATA_MCP_BASE = "https://mcp.brightdata.com/mcp"
BRIGHTDATA_MCP_PARAMS = f"?token={BRIGHTDATA_TOKEN}&groups=advanced_scraping&tools=web_data_zillow_properties_listing"

# BrightData Proxy (for Playwright routing — needs KYC for Zillow)
BRIGHTDATA_PROXY_HOST = "brd.superproxy.io"
BRIGHTDATA_PROXY_PORT = 33335
BRIGHTDATA_PROXY_USER = os.environ.get("BRIGHTDATA_PROXY_USER", "")
BRIGHTDATA_PROXY_PASS = os.environ.get("BRIGHTDATA_PROXY_PASS", "")

# HouseCanary
HOUSECANARY_API_KEY = os.environ.get("HOUSECANARY_API_KEY", "")
HOUSECANARY_API_SECRET = os.environ.get("HOUSECANARY_API_SECRET", "")
HOUSECANARY_BASE = "https://api.housecanary.com/v2"

# BatchData
BATCHDATA_API_KEY = os.environ.get("BATCHDATA_API_KEY", "")
BATCHDATA_BASE = "https://api.batchdata.com/api/v1"

# Cloudflare
CLOUDFLARE_API_KEY = os.environ.get("CLOUDFLARE_API_KEY", "")
CLOUDFLARE_ACCOUNT_ID = os.environ.get("CLOUDFLARE_ACCOUNT_ID", "")
CLOUDFLARE_EMAIL = os.environ.get("CLOUDFLARE_EMAIL", "")

# URL routing rules — which fetcher to use for which domain
ROUTE_RULES = {
    "zillow.com": {
        "pdp": ["brightdata_zillow", "curl_cffi", "firecrawl", "browserbase", "playwright"],
        "search": ["browserbase", "curl_cffi", "firecrawl", "brightdata_proxy_playwright"],
    },
    "redfin.com": ["firecrawl", "browserbase", "playwright", "curl_cffi"],
    "realtor.com": ["firecrawl", "browserbase", "playwright", "curl_cffi"],
    "bid4assets.com": ["firecrawl", "browserbase", "playwright", "cloudflare_crawl"],
    "realforeclose.com": ["firecrawl", "browserbase", "playwright", "cloudflare_crawl"],
    "cookcountyil.gov": ["firecrawl", "cloudflare_crawl", "browserbase", "playwright"],
    "franklincountyauditor.com": ["firecrawl", "cloudflare_crawl", "browserbase", "playwright"],
    "_default": ["curl_cffi", "firecrawl", "browserbase", "playwright", "cloudflare_crawl"],
    "_gov": ["firecrawl", "cloudflare_crawl", "browserbase", "playwright", "requests"],
}

# Validation thresholds
VALIDATION = {
    "price_divergence_pct": 50,    # flag if sources disagree by >50%
    "rent_divergence_pct": 25,     # flag if rent estimates diverge >25%
    "min_sources_for_verified": 2, # need at least 2 agreeing sources for "verified" badge
    "confidence_weights": {
        "housecanary": 0.4,        # HC is most reliable for valuations
        "brightdata_zillow": 0.35, # BrightData pulls live Zillow data
        "batchdata": 0.25,         # Tax assessed is often stale but real
    },
}

# Firecrawl
FIRECRAWL_API_KEY = os.environ.get("FIRECRAWL_API_KEY", "")
FIRECRAWL_BASE = "https://api.firecrawl.dev/v1"

# Browserbase
BROWSERBASE_API_KEY = os.environ.get("BROWSERBASE_API_KEY", "")
BROWSERBASE_PROJECT_ID = os.environ.get("BROWSERBASE_PROJECT_ID", "")

# Rate limiting
RATE_LIMITS = {
    "brightdata_zillow": {"delay_s": 2.0, "max_per_min": 25},
    "housecanary": {"delay_s": 1.0, "max_per_min": 30},  # 250 components/min, batch 30
    "batchdata": {"delay_s": 0.5, "max_per_min": 60},
    "cloudflare_crawl": {"delay_s": 1.0, "max_per_min": 30},
    "curl_cffi": {"delay_s": 0.3, "max_per_min": 60},
    "playwright": {"delay_s": 2.0, "max_per_min": 15},
    "firecrawl": {"delay_s": 1.0, "max_per_min": 30},
    "browserbase": {"delay_s": 3.0, "max_per_min": 10},
}
