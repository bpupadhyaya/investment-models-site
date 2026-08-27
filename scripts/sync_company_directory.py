#!/usr/bin/env python3
"""
Build/maintain the public directory of every currently-listed SEC-registered public company,
enriched with its SIC Major Group, for this site's "browse companies" teaser page.

This is an INDEPENDENT copy of the same script that lives in the private pvt/investment-models
repo (scripts/sync_company_directory.py there). That's deliberate, not duplication-by-accident:
the source data (SEC's own public filings) is free and public, so there's nothing to leak by
fetching it twice, and it avoids needing a cross-repo write credential from the private repo
into this public one. See that repo's MISSION.md / sectors/sector-map.md for the full reasoning
(decided 2026-08-29-ish).

Two free SEC endpoints, no API key (just a descriptive User-Agent):
  - www.sec.gov/files/company_tickers.json      -- full ticker/CIK/name universe (~8,000 unique
                                                    companies), ONE request.
  - data.sec.gov/submissions/CIK##########.json -- per-company SIC code, ONE request PER company.

The second endpoint is the expensive part: ~8,000 companies at SEC's fair-access rate limit
means this cannot be a single fast run. This script is therefore INCREMENTAL and RESUMABLE:
  - The output file doubles as the cache. Any CIK already present is never re-fetched (a
    company's SIC code essentially never changes, so this is safe, not just an optimization).
  - Each run only looks up CIKs it hasn't seen before (new IPOs since the last run), capped at
    MAX_NEW_LOOKUPS_PER_RUN so a single run can't monopolize the rate limit or run forever.
  - On the very first run against an empty output file, this means the ~8,000-company backlog
    fills in over several runs, not all at once -- that's expected, not a bug.

Also cross-references data/covered-tickers.json (hand-maintained: which tickers actually have a
model built and for sale in the private repo, e.g. HOOD/MRNA/TSLA/NVDA today) so each company
record in the output already carries a "status": "available" | "coming_soon" flag -- the
teaser/paywall UI reads that field directly rather than joining two files client-side. Update
covered-tickers.json by hand whenever a new company model ships; this script does NOT talk to
the private repo at all.

Usage: python3 scripts/sync_company_directory.py
Reads:  data/covered-tickers.json (hand-maintained)
Writes: data/companies.json
"""
import json
import re
import subprocess
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_PATH = REPO_ROOT / "data" / "companies.json"
COVERED_TICKERS_PATH = REPO_ROOT / "data" / "covered-tickers.json"

COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
SUBMISSIONS_URL_TMPL = "https://data.sec.gov/submissions/CIK{cik:010d}.json"
USER_AGENT = "investment-models-site-sync/1.0 (github.com/bpupadhyaya/investment-models-site)"

# Cap on new (never-before-seen) CIK lookups per run. At ~6/sec (see REQUEST_DELAY_SECONDS)
# this is well under a GitHub Actions job's time budget, and safely under SEC's fair-access
# limit even accounting for retries. Raise this if a faster initial backfill is wanted, but
# stay conservative -- getting SEC's IP-level rate limiter to trigger costs everyone using it
# from the same runner pool, not just this job.
MAX_NEW_LOOKUPS_PER_RUN = 1500
REQUEST_DELAY_SECONDS = 0.15  # ~6.6 req/sec, under SEC's ~10 req/sec fair-access ceiling

# SEC's rate-limit/block windows have been observed to last hours during this project's
# development (not the ~10-minute window SEC's own docs describe), so retries need real backoff
# to have any chance of landing after the window clears, not just to smooth over jitter. This
# still fits well inside a scheduled job's time budget (~5 min worst case for the one bulk
# company_tickers.json call, which is retried harder since the whole run depends on it).
MAX_RETRIES = 3
BULK_FETCH_MAX_RETRIES = 5
BULK_FETCH_BACKOFF_SECONDS = [10, 20, 40, 80, 160]

# Fallback titles for SIC major groups with no round-number ("XX00") code in SEC's active
# code list. Source: OSHA SIC Manual, https://www.osha.gov/data/sic-manual (authoritative,
# government-published, frozen since the 1987 SIC revision).
OSHA_MAJOR_GROUP_TITLES = {
    "01": "Agricultural Production Crops",
    "02": "Agricultural Production Livestock And Animal Specialties",
    "07": "Agricultural Services",
    "08": "Forestry",
    "09": "Fishing, Hunting, And Trapping",
    "10": "Metal Mining",
    "12": "Coal Mining",
    "13": "Oil And Gas Extraction",
    "14": "Mining And Quarrying Of Nonmetallic Minerals, Except Fuels",
    "15": "Building Construction General Contractors And Operative Builders",
    "16": "Heavy Construction Other Than Building Construction Contractors",
    "17": "Construction Special Trade Contractors",
    "20": "Food And Kindred Products",
    "21": "Tobacco Products",
    "22": "Textile Mill Products",
    "23": "Apparel And Other Finished Products Made From Fabrics And Similar Materials",
    "24": "Lumber And Wood Products, Except Furniture",
    "25": "Furniture And Fixtures",
    "26": "Paper And Allied Products",
    "27": "Printing, Publishing, And Allied Industries",
    "28": "Chemicals And Allied Products",
    "29": "Petroleum Refining And Related Industries",
    "30": "Rubber And Miscellaneous Plastics Products",
    "31": "Leather And Leather Products",
    "32": "Stone, Clay, Glass, And Concrete Products",
    "33": "Primary Metal Industries",
    "34": "Fabricated Metal Products, Except Machinery And Transportation Equipment",
    "35": "Industrial And Commercial Machinery And Computer Equipment",
    "36": "Electronic And Other Electrical Equipment And Components, Except Computer Equipment",
    "37": "Transportation Equipment",
    "38": "Measuring, Analyzing, And Controlling Instruments; Photographic, Medical And Optical Goods; Watches And Clocks",
    "39": "Miscellaneous Manufacturing Industries",
    "40": "Railroad Transportation",
    "41": "Local And Suburban Transit And Interurban Highway Passenger Transportation",
    "42": "Motor Freight Transportation And Warehousing",
    "43": "United States Postal Service",
    "44": "Water Transportation",
    "45": "Transportation By Air",
    "46": "Pipelines, Except Natural Gas",
    "47": "Transportation Services",
    "48": "Communications",
    "49": "Electric, Gas, And Sanitary Services",
    "50": "Wholesale Trade-Durable Goods",
    "51": "Wholesale Trade-Non-Durable Goods",
    "52": "Building Materials, Hardware, Garden Supply, And Mobile Home Dealers",
    "53": "General Merchandise Stores",
    "54": "Food Stores",
    "55": "Automotive Dealers And Gasoline Service Stations",
    "56": "Apparel And Accessory Stores",
    "57": "Home Furniture, Furnishings, And Equipment Stores",
    "58": "Eating And Drinking Places",
    "59": "Miscellaneous Retail",
    "60": "Depository Institutions",
    "61": "Non-Depository Credit Institutions",
    "62": "Security And Commodity Brokers, Dealers, Exchanges, And Services",
    "63": "Insurance Carriers",
    "64": "Insurance Agents, Brokers, And Service",
    "65": "Real Estate",
    "67": "Holding And Other Investment Offices",
    "70": "Hotels, Rooming Houses, Camps, And Other Lodging Places",
    "72": "Personal Services",
    "73": "Business Services",
    "75": "Automotive Repair, Services, And Parking",
    "76": "Miscellaneous Repair Services",
    "78": "Motion Pictures",
    "79": "Amusement And Recreation Services",
    "80": "Health Services",
    "81": "Legal Services",
    "82": "Educational Services",
    "83": "Social Services",
    "84": "Museums, Art Galleries, And Botanical And Zoological Gardens",
    "86": "Membership Organizations",
    "87": "Engineering, Accounting, Research, Management, And Related Services",
    "88": "Private Households",
    "89": "Miscellaneous Services",
    "91": "Executive, Legislative, And General Government, Except Finance",
    "92": "Justice, Public Order, And Safety",
    "93": "Public Finance, Taxation, And Monetary Policy",
    "94": "Administration Of Human Resource Programs",
    "95": "Administration Of Environmental Quality And Housing Programs",
    "96": "Administration Of Economic Programs",
    "97": "National Security And International Affairs",
    "99": "Nonclassifiable Establishments",
}


def curl_json(url):
    result = subprocess.run(
        ["curl", "-s", "-A", USER_AGENT, url],
        capture_output=True,
        text=True,
        timeout=30,
        check=True,
    )
    text = result.stdout
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        if "Request Rate Threshold Exceeded" in text:
            print(f"  [blocked] SEC rate limit hit for {url}")
            return None  # signal: rate-limited/blocked, caller decides whether to retry
        if "Undeclared Automated Tool" in text:
            print(f"  [blocked] SEC bot-detection flagged {url}")
            return None
        raise RuntimeError(f"Unexpected non-JSON response from {url}: {text[:200]!r}")


def fetch_with_retry(url, max_retries=MAX_RETRIES, backoff=None):
    for attempt in range(max_retries):
        data = curl_json(url)
        if data is not None:
            return data
        sleep_for = backoff[attempt] if backoff else 2 ** attempt * 5  # 5s, 10s, 20s
        if attempt < max_retries - 1:
            print(f"  retrying in {sleep_for}s ({attempt + 1}/{max_retries})...")
        time.sleep(sleep_for)
    return None  # gave up -- caller skips this CIK for this run, retries next run


def fetch_company_tickers():
    data = fetch_with_retry(
        COMPANY_TICKERS_URL, max_retries=BULK_FETCH_MAX_RETRIES, backoff=BULK_FETCH_BACKOFF_SECONDS
    )
    if data is None:
        # Low-stakes: this is a scheduled job. GitHub-hosted runners share IP ranges across
        # unrelated CI traffic worldwide, so SEC's per-IP rate limit can already be exhausted
        # by *other* jobs even when this script makes almost no requests itself -- this isn't
        # necessarily a bug. The next scheduled run (different runner, likely different IP)
        # will probably succeed; no action needed unless failures persist across many days.
        raise RuntimeError(
            "Could not fetch company_tickers.json after retries -- SEC is blocking this "
            "request right now (possibly via a shared GitHub Actions IP rate-limited by "
            "unrelated traffic, not necessarily this job). The next scheduled run will retry "
            "automatically; this is expected to be occasionally flaky, not a sign of a broken "
            "script."
        )
    return {str(v["cik_str"]): {"ticker": v["ticker"], "title": v["title"]} for v in data.values()}


def major_group_for_sic(sic_code):
    if not sic_code:
        return None, None
    prefix = sic_code.zfill(4)[:2]
    title = OSHA_MAJOR_GROUP_TITLES.get(prefix)
    return prefix, title


def load_existing_directory():
    if not OUTPUT_PATH.exists():
        return {}
    data = json.loads(OUTPUT_PATH.read_text())
    return {c["cik"]: c for c in data.get("companies", [])}


def load_covered_tickers():
    if not COVERED_TICKERS_PATH.exists():
        return {}
    data = json.loads(COVERED_TICKERS_PATH.read_text())
    return {c["ticker"]: c for c in data.get("covered", [])}


def sync():
    current_tickers = fetch_company_tickers()
    existing = load_existing_directory()
    covered = load_covered_tickers()

    new_ciks = [cik for cik in current_tickers if cik not in existing]
    to_fetch = new_ciks[:MAX_NEW_LOOKUPS_PER_RUN]
    remaining_after = len(new_ciks) - len(to_fetch)

    fetched = 0
    skipped = []
    for cik in to_fetch:
        url = SUBMISSIONS_URL_TMPL.format(cik=int(cik))
        data = fetch_with_retry(url)
        time.sleep(REQUEST_DELAY_SECONDS)
        if data is None:
            skipped.append(cik)
            continue
        sic = data.get("sic") or None
        major_group_code, major_group_title = major_group_for_sic(sic)
        filing_dates = data.get("filings", {}).get("recent", {}).get("filingDate", [])
        exchanges = data.get("exchanges") or []
        existing[cik] = {
            "cik": cik,
            "ticker": current_tickers[cik]["ticker"],
            "name": data.get("name") or current_tickers[cik]["title"],
            "sic": sic,
            "sic_description": data.get("sicDescription") or None,
            "major_group_code": major_group_code,
            "major_group_title": major_group_title,
            "exchanges": exchanges,
            # NOT the literal SEC registration date (that needs the actual registration
            # statement) -- this is the earliest filing date visible in SEC's "recent filings"
            # window for this CIK, a close, honestly-labeled proxy. For companies with very long
            # filing histories, SEC paginates older filings out of "recent" into separate
            # per-year files this script doesn't fetch (out of scope for now), so this can read
            # later than the company's true first filing in those cases.
            "earliest_recent_filing_date": min(filing_dates) if filing_dates else None,
            "active": True,
        }
        fetched += 1

    for cik, record in existing.items():
        record["active"] = cik in current_tickers
        if cik in current_tickers:
            record["ticker"] = current_tickers[cik]["ticker"]
        cover = covered.get(record["ticker"])
        record["status"] = "available" if cover else "coming_soon"
        record["teaser"] = cover.get("teaser") if cover else None

    companies = sorted(existing.values(), key=lambda c: c["ticker"])
    output = {
        "source": {
            "tickers": COMPANY_TICKERS_URL,
            "sic_lookup": SUBMISSIONS_URL_TMPL,
            "earliest_recent_filing_date_note": (
                "Earliest filing date in SEC's 'recent filings' window for this CIK -- a proxy "
                "for registration date, not the literal SEC registration date itself. Can read "
                "later than a company's true first filing if it has a very long filing history "
                "(SEC paginates older filings out of the 'recent' window)."
            ),
        },
        "total_current_tickers": len(current_tickers),
        "total_resolved": len(existing),
        "still_unresolved": remaining_after + len(skipped),
        "available_count": sum(1 for c in companies if c["status"] == "available"),
        "companies": companies,
    }
    OUTPUT_PATH.parent.mkdir(exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(output, indent=2) + "\n")

    print(f"Resolved {fetched} new compan{'y' if fetched == 1 else 'ies'} this run.")
    print(f"Total resolved so far: {len(existing)} / {len(current_tickers)} current tickers.")
    if skipped:
        print(f"{len(skipped)} lookups failed after retries -- will retry next run.")
    if remaining_after:
        print(f"{remaining_after} more new tickers queued for future runs (per-run cap reached).")


if __name__ == "__main__":
    sync()
