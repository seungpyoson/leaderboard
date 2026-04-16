# Graph Report - /Users/spson/Projects/Claude/leaderboard  (2026-04-16)

## Corpus Check
- Corpus is ~10,013 words - fits in a single context window. You may not need a graph.

## Summary
- 137 nodes · 220 edges · 8 communities detected
- Extraction: 77% EXTRACTED · 23% INFERRED · 0% AMBIGUOUS · INFERRED: 50 edges (avg confidence: 0.8)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- [[_COMMUNITY_Community 0|Community 0]]
- [[_COMMUNITY_Community 1|Community 1]]
- [[_COMMUNITY_Community 2|Community 2]]
- [[_COMMUNITY_Community 3|Community 3]]
- [[_COMMUNITY_Community 4|Community 4]]
- [[_COMMUNITY_Community 5|Community 5]]
- [[_COMMUNITY_Community 6|Community 6]]
- [[_COMMUNITY_Community 7|Community 7]]

## God Nodes (most connected - your core abstractions)
1. `main()` - 15 edges
2. `main()` - 9 edges
3. `fetch_capital()` - 8 edges
4. `save_state()` - 8 edges
5. `_fetch_phase()` - 8 edges
6. `main()` - 7 edges
7. `build_positions()` - 7 edges
8. `_book_pnl()` - 7 edges
9. `write_csv()` - 7 edges
10. `process_account()` - 7 edges

## Surprising Connections (you probably didn't know these)
- `Incremental State Persistence` --conceptually_related_to--> `main()`  [INFERRED]
  CLAUDE.md → /Users/spson/Projects/Claude/leaderboard/scraper/backfill_s3.py
- `__NEXT_DATA__ SSR Payload` --rationale_for--> `fetch_leaderboard()`  [INFERRED]
  CLAUDE.md → /Users/spson/Projects/Claude/leaderboard/scraper/fetch.py
- `__NEXT_DATA__ SSR Payload` --rationale_for--> `fetch_user_stats()`  [INFERRED]
  CLAUDE.md → /Users/spson/Projects/Claude/leaderboard/scraper/fetch.py
- `OrderFilled Events (Goldsky)` --conceptually_related_to--> `scan_s3_fills()`  [INFERRED]
  CLAUDE.md → /Users/spson/Projects/Claude/leaderboard/scraper/backfill_s3.py
- `No Raw Fills Storage Rule` --rationale_for--> `build_positions()`  [INFERRED]
  CLAUDE.md → /Users/spson/Projects/Claude/leaderboard/scraper/backfill_s3.py

## Communities

### Community 0 - "Community 0"
Cohesion: 0.1
Nodes (33): Exception, main(), fetch_fills(), _fetch_phase(), FetchAbortedError, load_state(), new_account_state(), process_account() (+25 more)

### Community 1 - "Community 1"
Cohesion: 0.15
Nodes (20): Candidate Discovery Workflow, auto_status(), classify_style(), fetch_leaderboard(), load_existing_candidates(), load_known_wallets(), main(), Load wallet addresses already in accounts.json (lowercased). (+12 more)

### Community 2 - "Community 2"
Cohesion: 0.16
Nodes (18): __NEXT_DATA__ SSR Payload, Scraper (Python/requests), extract_profile_data(), fetch_profile(), fmt_money(), fmt_pct(), _is_polymarket_host(), Shared utilities for Polymarket profile scraping.  Used by fetch.py (daily refre (+10 more)

### Community 3 - "Community 3"
Cohesion: 0.14
Nodes (13): Static Frontend, GitHub Actions Cron CI, GitHub Pages Hosting, Goldsky Pipeline, Polymarket Trader Leaderboard, OrderFilled Events (Goldsky), Rate Limit (0.5s between requests), Incremental State Persistence (+5 more)

### Community 4 - "Community 4"
Cohesion: 0.2
Nodes (15): Gamma API Market Resolution, No Raw Fills Storage Rule, Position Tracking (net_shares + cost_basis), Welford Running Aggregates (Sharpe), _book_pnl(), build_positions(), main(), Iterate timestamp-sorted fills, update account states.      Each row tuple: (wal (+7 more)

### Community 5 - "Community 5"
Cohesion: 0.2
Nodes (15): Polymarket Proxy Wallets, No API Keys (Public Endpoints), _build_sql(), _execute_sql(), fetch_capital(), _get_api_key(), _headers(), _normalize_address() (+7 more)

### Community 6 - "Community 6"
Cohesion: 0.29
Nodes (7): Candidate Scoring Signals, Efficiency Signal (25%), Profile Enrichment (10%), Markets Diversification (15%), PnL Magnitude Signal (30%), $50K Candidate PnL Threshold, Trade Count Signal (20%)

### Community 7 - "Community 7"
Cohesion: 0.4
Nodes (5): atomic_write(), Write content to file atomically via temp file + rename.      Shared by discover, main(), Core promotion logic — must be called under lock., _run_promotion()

## Knowledge Gaps
- **55 isolated node(s):** `Paginate the crypto leaderboard API. Returns up to `limit` traders.`, `Fetch per-user stats (all-market, not crypto-filtered).      Returns: {"trades":`, `Read all S3 parquet partitions, filter to tracked wallets, return rows.      Ret`, `Iterate timestamp-sorted fills, update account states.      Each row tuple: (wal`, `Resolve markets and book PnL for all 28 accounts.` (+50 more)
  These have ≤1 connection - possible missing edges or undocumented components.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `Scraper (Python/requests)` connect `Community 2` to `Community 1`, `Community 3`?**
  _High betweenness centrality (0.207) - this node is a cross-community bridge._
- **Why does `main()` connect `Community 2` to `Community 5`?**
  _High betweenness centrality (0.184) - this node is a cross-community bridge._
- **Why does `Polymarket Trader Leaderboard` connect `Community 3` to `Community 2`?**
  _High betweenness centrality (0.182) - this node is a cross-community bridge._
- **Are the 5 inferred relationships involving `main()` (e.g. with `to_float()` and `fmt_money()`) actually correct?**
  _`main()` has 5 INFERRED edges - model-reasoned connections that need verification._
- **Are the 4 inferred relationships involving `main()` (e.g. with `load_state()` and `save_state()`) actually correct?**
  _`main()` has 4 INFERRED edges - model-reasoned connections that need verification._
- **Are the 4 inferred relationships involving `save_state()` (e.g. with `_book_pnl()` and `resolve_elapsed_markets()`) actually correct?**
  _`save_state()` has 4 INFERRED edges - model-reasoned connections that need verification._
- **What connects `Paginate the crypto leaderboard API. Returns up to `limit` traders.`, `Fetch per-user stats (all-market, not crypto-filtered).      Returns: {"trades":`, `Read all S3 parquet partitions, filter to tracked wallets, return rows.      Ret` to the rest of the system?**
  _55 weakly-connected nodes found - possible documentation gaps or missing edges._