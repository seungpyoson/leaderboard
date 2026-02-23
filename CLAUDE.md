# Leaderboard — Polymarket Trader Performance

Public static site showing PnL, volume, and efficiency for top Polymarket crypto interval traders.

## Stack
- **Scraper**: Python (requests) — scrapes Polymarket profile page SSR data
- **Frontend**: Static HTML/CSS/JS — sortable table, no framework
- **Hosting**: GitHub Pages (free) with custom domain
- **CI**: GitHub Actions cron — daily data refresh

## Architecture
```
scraper/fetch.py  →  data/profiles.json  →  index.html (reads JSON, renders table)
                                          └  GitHub Actions auto-commits updated JSON
```

## Rules
- No API keys needed — Polymarket profiles are public
- Scraper outputs JSON (not markdown) — frontend consumes it
- Rate limit: 0.5s between requests minimum
- All metrics sourced from Polymarket SSR `__NEXT_DATA__` payload
