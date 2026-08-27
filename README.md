# investment-models-site

Public teaser/marketing site for [`pvt/investment-models`](https://github.com/bpupadhyaya/investment-models)
(private) — entices visitors to buy predictive investment models by showing the breadth of
companies covered (all SEC-registered public companies) and which ones already have a real,
backtested model available.

## Data pipeline

`scripts/sync_company_directory.py` builds `data/companies.json`: every currently-listed
SEC-registered company (~8,000), each tagged with its SIC Major Group and a `status` of
`"available"` or `"coming_soon"`. It's an **independent copy** of the same script in the private
repo — deliberately not shared/pushed between repos, since the source data (SEC's own public
filings) is free and public, so there's nothing to leak by fetching it twice, and it avoids a
cross-repo write credential. See that repo's `MISSION.md` for the full reasoning.

- **Runs daily** (`.github/workflows/sync-companies.yml`) — companies register/delist with SEC
  daily, so this needs to be more responsive than a weekly/monthly cadence, though nowhere near
  the ~4h cadence reserved for genuinely fast-moving data.
- **Incremental/resumable**: the per-company SIC lookup is one SEC request per company (~8,000
  total), so the first run's backlog fills in over several days, not all at once. Already-resolved
  companies are never re-fetched.
- `data/covered-tickers.json` is **hand-maintained**, not auto-generated: list a ticker there
  only once its model has actually been built and backward-tested in the private repo (not just
  case-studied/planned) — this starts **empty**, since no model has shipped yet as of this
  writing. Never mark a ticker "available" before there's a real model behind it.

## Status

Repo scaffolded, data pipeline built, no site pages built yet — that's the next step, once the
first company model actually ships in the private repo.
