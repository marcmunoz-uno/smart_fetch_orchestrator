"""Sanity test for the FL Save-Our-Homes validator fix."""
import sys
from pathlib import Path

_PKG_ROOT = Path(__file__).resolve().parent.parent
if str(_PKG_ROOT) not in sys.path:
    sys.path.insert(0, str(_PKG_ROOT))

from smart_fetch.validator import validate_property  # noqa: E402


CASES = [
    {
        "label": "FL Save-Our-Homes (should NOT flag price_divergence)",
        "prop": {
            "listing_price": 400_000,    # Zillow market
            "hc_avm_mean": 410_000,      # HC market (close)
            "bd_tax_assessed": 180_000,  # FL tax-assessed (~45% of market — SOH)
        },
    },
    {
        "label": "Real market divergence (SHOULD flag)",
        "prop": {
            "listing_price": 400_000,
            "hc_avm_mean": 650_000,      # 60%+ divergence between market sources
        },
    },
    {
        "label": "Only tax-assessed (best_price falls back to tax)",
        "prop": {
            "bd_tax_assessed": 120_000,
        },
    },
    {
        "label": "Tax 3x market (wrong-property-matched sanity flag)",
        "prop": {
            "listing_price": 200_000,
            "hc_avm_mean": 210_000,
            "bd_tax_assessed": 700_000,
        },
    },
    {
        "label": "Normal 3-source agreement",
        "prop": {
            "listing_price": 300_000,
            "hc_avm_mean": 295_000,
            "bd_tax_assessed": 285_000,  # not FL-capped; all three agree
        },
    },
]


def main():
    for c in CASES:
        v = validate_property(c["prop"])
        divergence_flag = [f for f in v["flags"] if "price_divergence" in f]
        tax_flag = [f for f in v["flags"] if "tax_far_above" in f]
        print(f"\n{c['label']}")
        print(f"  input: {c['prop']}")
        print(f"  best_price: {v['best_price']}")
        print(f"  sources_used: {v['sources_used']}")
        print(f"  confidence: {v['confidence']}")
        print(f"  validated: {v['validated']}")
        print(f"  divergence flag: {divergence_flag or 'none ✓' }")
        if tax_flag:
            print(f"  tax sanity flag: {tax_flag}")


if __name__ == "__main__":
    main()
