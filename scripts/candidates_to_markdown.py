"""Convert portal_candidates_*.json into a human-reviewable markdown file."""
import json
import sys
from pathlib import Path
from urllib.parse import urlparse

SRC = Path(__file__).parent / "portal_candidates_2026-04-22.json"
DEST = Path.home() / "Desktop" / "portal_candidates_review_2026-04-22.md"


STATE_KWS = {
    'GA': ['ga', 'georgia', 'atlanta', 'fulton', 'bibb', 'macon'],
    'FL': ['fl', 'florida', 'tampa', 'miami', 'hillsborough', 'dade', 'orange',
           'orlando', 'duval', 'jacksonville', 'hcfl'],
    'MI': ['mi', 'michigan', 'wayne', 'ingham', 'lansing', 'detroit'],
    'PA': ['pa', 'penn', 'philadelphia', 'phila', 'allegheny', 'pittsburgh'],
    'MO': ['mo', 'missouri', 'louis', 'jackson', 'kcmo'],
    'OH': ['oh', 'ohio', 'cuyahoga', 'cleveland', 'franklin', 'columbus', 'lucas',
           'toledo', 'summit', 'akron', 'lorain', 'montgomery', 'dayton'],
    'TX': ['tx', 'texas', 'dallas', 'harris', 'houston'],
    'TN': ['tn', 'tennessee', 'shelby', 'memphis'],
    'IL': ['il', 'illinois', 'cook', 'chicago'],
    'IN': ['in', 'indiana', 'marion', 'indianapolis', 'indy'],
    'AL': ['al', 'alabama', 'jefferson', 'birmingham'],
    'MS': ['ms', 'mississippi', 'hinds'],
    'AZ': ['az', 'arizona', 'maricopa', 'phoenix'],
    'WI': ['wi', 'wisconsin', 'milwaukee'],
    'OK': ['ok', 'oklahoma', 'tulsa'],
    'LA': ['la', 'louisiana', 'orleans', 'baton', 'rouge'],
    'AR': ['ar', 'arkansas', 'pulaski', 'little', 'rock'],
}

DEAL_ANTIPATTERNS = {
    'land_bank': ['probate', 'sheriff', 'court', 'recorder', 'deed',
                  'realforeclose', 'foreclos'],
    'probate': ['land', 'landbank', 'foreclos', 'sheriff'],
    'foreclosure': ['probate', 'landbank'],
    'lis_pendens': ['probate', 'landbank'],
}


def bucket(c):
    if not c.get("alive"):
        return "SOFT"
    if c["score"] >= 100:
        return "HIGH"
    if c["score"] >= 40:
        return "MED"
    return "LOW"


def warnings(item, top):
    w = []
    url = top["url"].lower()
    netloc = urlparse(url).netloc.lower()
    state_kws = STATE_KWS.get(item["state"], [])
    if state_kws and not any(k in netloc for k in state_kws) and top["score"] > 0:
        w.append(f"state mismatch? ({item['state']} vs {netloc})")
    for anti in DEAL_ANTIPATTERNS.get(item["deal"], []):
        if anti in url and anti not in item["deal"]:
            w.append(f"deal mismatch: contains '{anti}'")
            break
    return w


def main():
    results = json.loads(SRC.read_text())
    results.sort(key=lambda r: (r["market"], r["deal"]))

    lines = []
    lines.append("# County Portal URL Candidates — Review")
    lines.append("")
    lines.append(f"Generated from `{SRC.name}` — {len(results)} gaps.")
    lines.append("")
    lines.append("**How to review:** for each gap below, look at the top candidate.")
    lines.append("- If the URL looks correct → check the box `[x]` next to **Candidate 1**.")
    lines.append("- If Candidate 1 is wrong but 2 or 3 looks right → check that one instead.")
    lines.append("- If none look right → write a URL under \"Manual URL\", or leave blank to skip this gap.")
    lines.append("")
    lines.append("**Quality legend:**")
    lines.append("- **HIGH** — .gov or known-pattern domain, 2xx response. Low risk.")
    lines.append("- **MED** — state/partial-gov domain, 2xx response.")
    lines.append("- **LOW** — non-.gov and not a recognized pattern. Eyeball the URL/title.")
    lines.append("- **SOFT** — likely correct but plain GET was blocked (403/timeout). Firecrawl will probably rescue it.")
    lines.append("")

    # Quality counts
    high = med = low = soft = none = 0
    for r in results:
        if not r["candidates"]:
            none += 1
            continue
        b = bucket(r["candidates"][0])
        if b == "HIGH": high += 1
        elif b == "MED": med += 1
        elif b == "LOW": low += 1
        elif b == "SOFT": soft += 1

    lines.append("## Summary")
    lines.append("")
    lines.append(f"| Bucket | Count |")
    lines.append(f"|--------|-------|")
    lines.append(f"| HIGH   | {high} |")
    lines.append(f"| MED    | {med} |")
    lines.append(f"| LOW    | {low} |")
    lines.append(f"| SOFT   | {soft} |")
    lines.append(f"| none   | {none} |")
    lines.append(f"| **total** | **{len(results)}** |")
    lines.append("")
    lines.append("---")
    lines.append("")

    # Group by market
    by_market = {}
    for r in results:
        by_market.setdefault(r["market"], []).append(r)

    for market in sorted(by_market.keys()):
        gaps = by_market[market]
        county = gaps[0].get("county", "")
        state = gaps[0].get("state", "")
        lines.append(f"## {market}  ({county} County, {state})")
        lines.append("")

        for g in gaps:
            deal = g["deal"]
            lines.append(f"### {deal}")
            lines.append("")
            if g.get("had_broken_url"):
                lines.append(f"*Previously broken URL: `{g['had_broken_url']}`*")
                lines.append("")
            lines.append(f"*Query: {g['query']}*")
            lines.append("")

            if not g["candidates"]:
                lines.append("- No candidates found.")
                lines.append("- [ ] Manual URL: `_________________`")
                lines.append("")
                continue

            for i, c in enumerate(g["candidates"], 1):
                b = bucket(c)
                status = c.get("status", "?")
                final = f" → `{c['final_url']}`" if c.get("final_url") else ""
                flags = warnings(g, c) if i == 1 else []
                flag_str = "  ⚠ " + "; ".join(flags) if flags else ""
                lines.append(f"- [ ] **Candidate {i}** — `[{b}]` score={c['score']} status={status}{flag_str}")
                lines.append(f"    - URL: {c['url']}{final}")
                if c.get("title"):
                    lines.append(f"    - Title: {c['title']}")
                if c.get("description"):
                    desc = c["description"].replace("\n", " ").strip()
                    lines.append(f"    - Snippet: {desc}")
                lines.append("")

            lines.append(f"- [ ] Manual URL: `_________________`")
            lines.append(f"- [ ] Skip this gap")
            lines.append("")

        lines.append("---")
        lines.append("")

    DEST.write_text("\n".join(lines))
    size_kb = DEST.stat().st_size // 1024
    print(f"Wrote {DEST} ({size_kb} KB, {len(lines)} lines)")


if __name__ == "__main__":
    main()
