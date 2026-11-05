# leaderboard

- A failed or short leaderboard fetch or missing capital must not replace `data/profiles.json`. Current `scraper/fetch.py` still replaces it in those cases; fix that path before treating or committing a refresh as complete.
- Keep `scraper/goldsky_state.json` tracked for cursors and aggregates; do not persist raw fills.
