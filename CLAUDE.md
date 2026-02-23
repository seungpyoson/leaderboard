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
Goldsky GraphQL API          Gamma API (resolution)
       |                              |
       v                              v
goldsky_pipeline.py ──> goldsky_state.json (cursors + positions + aggregates)
       |
       v
dune-performance.csv ──> fetch.py ──> profiles.json ──> index.html
```

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
- State file must be committed — it holds cursors and running aggregates for incremental refresh
- Never store raw fills — process into aggregates and discard (gabagool22 = 14M fills)
