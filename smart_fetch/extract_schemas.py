"""Per-platform JSON schemas for Firecrawl /extract.

Each county portal in the spider config tags itself with `platform: <name>`.
The firecrawl_extract fetcher looks up the schema here and passes it to the
extract API. Output fields are designed to match the spider's standard property
record shape (address, case_number, price/opening_bid, sale_date, parcel_id).

Adding a new platform:
  1. Add an entry to PLATFORM_SCHEMAS below
  2. Tag config entries with `platform: <new_name>` and `method: firecrawl_extract`
  3. Save 2-3 sample HTML pages to tests/fixtures/<platform>/ so future drift
     can be caught with a parser test
"""

# A single listing's record schema — most platforms produce a list of these.
_LISTING_PROPS = {
    "address":      {"type": "string", "description": "Full street address"},
    "city":         {"type": "string"},
    "state":        {"type": "string"},
    "zip_code":     {"type": "string"},
    "case_number":  {"type": "string", "description": "Court case, docket, or filing number"},
    "parcel_id":    {"type": "string", "description": "County parcel / PIN / folio / APN"},
    "opening_bid":  {"type": "string", "description": "Minimum/opening bid in dollars (no commas/symbols)"},
    "judgment":     {"type": "string", "description": "Judgment amount if shown"},
    "assessed_value": {"type": "string"},
    "sale_date":    {"type": "string", "description": "Auction or sale date in YYYY-MM-DD or as-shown"},
    "filed_date":   {"type": "string"},
    "status":       {"type": "string", "description": "e.g. scheduled, sold, cancelled, withdrawn"},
    "plaintiff":    {"type": "string"},
    "defendant":    {"type": "string"},
    "owner":        {"type": "string"},
    "url":          {"type": "string", "description": "Detail/listing URL"},
}

_LISTINGS_ARRAY = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": _LISTING_PROPS,
    },
}


PLATFORM_SCHEMAS = {
    "realauction": {
        # Used by *.realforeclose.com (FL) and *.sheriffsaleauction.ohio.gov (OH).
        # Pages typically have a table of upcoming/past auctions.
        "prompt": (
            "This is a county sheriff sale auction page on the RealAuction platform. "
            "Extract every auction listing visible on the page. For each listing, capture "
            "the property address, case number, parcel ID, opening bid (in dollars, no "
            "currency symbols), assessed value if shown, sale date, plaintiff/defendant, "
            "and the listing detail URL if available. If a field is not visible, leave it empty."
        ),
        "schema": {
            "type": "object",
            "properties": {"listings": _LISTINGS_ARRAY},
        },
    },

    "epropertyplus": {
        # ePropertyPlus tenants: public-fcca, public-kclb, public-lans, etc.
        # Land bank inventory — properties for sale or under disposition.
        "prompt": (
            "This is a land bank property inventory page on the ePropertyPlus platform. "
            "Extract every property listed. For each property, capture the address, "
            "parcel ID / PIN, asking price or assessed value, current status (available, "
            "pending, under contract, sold), zoning if shown, and the detail URL. "
            "Skip image-only entries."
        ),
        "schema": {
            "type": "object",
            "properties": {"listings": _LISTINGS_ARRAY},
        },
    },

    "bid4assets": {
        # bid4assets.com auction listings (Philadelphia foreclosures, Wayne County, etc.)
        "prompt": (
            "This is a Bid4Assets auction listing page for tax/foreclosure auctions. "
            "Extract every property/auction visible. Capture property address, parcel ID, "
            "minimum bid (dollars only, no commas), auction start/end date, current bid "
            "if shown, and the auction URL. Skip non-property auctions (vehicles, etc.)."
        ),
        "schema": {
            "type": "object",
            "properties": {"listings": _LISTINGS_ARRAY},
        },
    },

    "civilview": {
        # salesweb.civilview.com — Louisiana parishes use this for foreclosure sales.
        "prompt": (
            "This is a sheriff sales listing page on the CivilView platform. Extract every "
            "scheduled sale visible. For each sale, capture the property address, case number, "
            "approximate judgment amount, sale date, plaintiff (often a bank/lender), and "
            "sale status (scheduled, postponed, sold)."
        ),
        "schema": {
            "type": "object",
            "properties": {"listings": _LISTINGS_ARRAY},
        },
    },

    "gsccca_lien": {
        # search.gsccca.org/lien/lienindex.asp — Georgia statewide lien index.
        # Covers foreclosure deeds + lis pendens.
        "prompt": (
            "This is the Georgia GSCCCA Lien Index search results page. Extract every lien "
            "record visible. Capture the lien type (foreclosure, lis pendens, tax), the "
            "grantor (defendant/owner), grantee (plaintiff/lender), property address if "
            "shown, county, file/instrument number, and filing date."
        ),
        "schema": {
            "type": "object",
            "properties": {"listings": _LISTINGS_ARRAY},
        },
    },

    "oscn_dockets": {
        # oscn.net — Oklahoma State Court Network. Used for probate and foreclosure.
        "prompt": (
            "This is an Oklahoma State Court Network (OSCN) docket search results page. "
            "Extract every case visible. Capture the case number, case type (probate, "
            "foreclosure, civil), filing date, parties (plaintiff/petitioner and defendant/"
            "respondent), property address if shown in the case caption, and the case URL."
        ),
        "schema": {
            "type": "object",
            "properties": {"listings": _LISTINGS_ARRAY},
        },
    },

    "civicengage_table": {
        # Generic catch-all for CivicEngage-CMS gov sites with foreclosure/sales tables.
        # Used by Shelby County Trustee, Allegheny Sheriff, etc.
        "prompt": (
            "This is a county or municipal government page listing properties for "
            "foreclosure, tax sale, or auction. Extract every property visible. Capture "
            "address, case/parcel number, sale date, opening bid or sale price, status, "
            "and any detail URL. Ignore administrative content (FAQs, instructions, "
            "contact info)."
        ),
        "schema": {
            "type": "object",
            "properties": {"listings": _LISTINGS_ARRAY},
        },
    },
}


def get_schema(platform: str) -> dict:
    """Return {prompt, schema} for a platform, or None."""
    return PLATFORM_SCHEMAS.get(platform)


def list_platforms() -> list:
    """Return the registered platform names."""
    return sorted(PLATFORM_SCHEMAS.keys())
