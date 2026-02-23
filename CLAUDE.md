# Leaderboard — Polymarket Trader Performance

Public static site showing PnL, volume, and efficiency for top Polymarket crypto interval traders.

## Stack
- **Scraper**: Python (requests) — scrapes Polymarket profile page SSR data
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
```

## Scripts
- `scraper/common.py` — shared: `to_float`, `extract_profile_data`, `fetch_profile`, `fmt_money`, `fmt_pct`
- `scraper/fetch.py` — daily: scrape profiles for tracked accounts → `data/profiles.json`
- `scraper/discover.py` — weekly: paginate Polymarket leaderboard API, filter/enrich new traders → `data/candidates.json`
- `scraper/promote.py` — manual: move `status: "approved"` candidates into `scraper/accounts.json`

## Candidate Discovery Workflow
1. `discover.py` runs weekly (Monday 9AM UTC) or manually via GitHub Actions
2. Auto-scoring assigns confidence scores: `auto-approved` (>= 0.75), `auto-rejected` (<= 0.30), `pending` (between)
3. Open `review.html` to review candidates — approve/reject with one click, export updated JSON
4. Run `python3 scraper/promote.py` to add approved/auto-approved candidates to `accounts.json`

## Candidate Scoring Signals
- PnL magnitude (30%) — raw profitability, $500K+ = max score
- Efficiency (25%) — PnL/volume ratio, 5%+ = max score
- Trade count (20%) — filters lucky one-shot traders, 200+ = max score
- Markets (15%) — diversification, 20+ = max score
- Profile enrichment (10%) — binary, confirms real active profile

## Rules
- No API keys needed — Polymarket profiles are public
- Scraper outputs JSON (not markdown) — frontend consumes it
- Rate limit: 0.5s between requests minimum
- All metrics sourced from Polymarket SSR `__NEXT_DATA__` payload
- Candidate PnL threshold: $50K minimum
- Note: Polymarket uses proxy wallets (smart contracts) for all users — EOA checks are not applicable
