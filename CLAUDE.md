# Leaderboard — Polymarket Trader Performance

Public static site showing PnL, volume, and efficiency for top Polymarket crypto interval traders.

## Stack
- **Scraper**: Python (requests) — scrapes Polymarket profile page SSR data
- **Goldsky pipeline**: Python — fetches on-chain fill data, computes PnL/Sharpe/metrics
- **Frontend**: Static HTML/CSS/JS — sortable table, no framework
- **Hosting**: GitHub Pages (free) with custom domain
- **CI**: GitHub Actions cron — daily data refresh

## Architecture
```
scraper/common.py      — shared utilities (profile scraping, formatting)
scraper/fetch.py       →  data/profiles.json  →  index.html (renders table)
scraper/discover.py    →  data/candidates.json    (weekly candidate discovery)
scraper/promote.py     — candidates.json → accounts.json promotion
                       └  GitHub Actions auto-commits updated JSON

Goldsky GraphQL API          Gamma API (resolution)
       |                              |
       v                              v
goldsky_pipeline.py ──> goldsky_state.json (cursors + positions + aggregates)
       |
       v
dune-performance.csv ──> fetch.py ──> profiles.json ──> index.html
```

## Scripts
- `scraper/common.py` — shared: `to_float`, `extract_profile_data`, `fetch_profile`, `fmt_money`, `fmt_pct`
- `scraper/fetch.py` — daily: scrape profiles for tracked accounts → `data/profiles.json`
- `scraper/discover.py` — weekly: paginate Polymarket leaderboard API, filter/enrich new traders → `data/candidates.json`
- `scraper/promote.py` — manual: move `status: "approved"` candidates into `scraper/accounts.json`

## Candidate Discovery Workflow
1. `discover.py` runs weekly (Monday 9AM UTC) or manually via GitHub Actions
2. Review `data/candidates.json` — set `status` to `"approved"` or `"rejected"`
3. Run `python3 scraper/promote.py` to add approved candidates to `accounts.json`

## Goldsky Pipeline (`scraper/goldsky_pipeline.py`)
- Fetches OrderFilled events from Goldsky subgraph (id_gt pagination, maker/taker split)
- Tracks positions per token_id (net_shares + cost_basis)
- Resolves markets via Gamma API (`closed=true`), computes PnL on resolution
- Running aggregates: Welford for Sharpe, win/loss counters, top markets
- State persisted in `goldsky_state.json` (committed for CI incremental refresh)
- `--max-minutes N` time budget for CI; processes smallest accounts first
- `--accounts NAME,...` to filter specific accounts

## Rules
- No API keys needed — Polymarket profiles and Goldsky subgraph are public
- Scraper outputs JSON (not markdown) — frontend consumes it
- Rate limit: 0.5s between requests minimum (both Goldsky and Gamma)
- All metrics sourced from Polymarket SSR `__NEXT_DATA__` payload
- Candidate PnL threshold: $50K minimum
- Note: Polymarket uses proxy wallets (smart contracts) for all users — EOA checks are not applicable
- State file must be committed — it holds cursors and running aggregates for incremental refresh
- Never store raw fills — process into aggregates and discard (gabagool22 = 14M fills)
