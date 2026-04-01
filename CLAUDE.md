# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

`abs-pro` collects and analyzes data on MLB's 2026 ABS (Automated Ball-Strike) challenge system. Opening Day was March 26, 2026 — there is no MLB-level ABS data before that date. AAA data from 2024–2025 exists on Baseball Savant but is a separate dataset.

## Stack

- **Python** — data collection and ETL
- **R** — analysis (written by the user; do not generate analysis code unless asked)
- **DuckDB** — local analytical database at `data/abs.duckdb`

## Commands

```bash
# Install dependencies
pip3 install -r requirements.txt

# Initialize the database
python3 src/collect/db.py

# Backfill from Opening Day to today
PYTHONPATH=src/collect python3 src/collect/gumbo.py

# Pull today's Savant leaderboard snapshot
PYTHONPATH=src/collect python3 src/collect/savant.py

# Run the daily job (GUMBO yesterday + Savant leaderboard)
PYTHONPATH=src/collect python3 src/jobs/daily.py

# Backfill a specific date range
PYTHONPATH=src/collect python3 src/collect/gumbo.py 2026-03-26 2026-04-15
```

## Data Sources

**MLB Stats API (GUMBO)** — `src/collect/gumbo.py`
- Endpoint: `GET /api/v1/game/{gamePk}/playByPlay`
- ABS challenges appear as pitches with a `reviewDetails` object
- `reviewType: "MJ"` distinguishes ABS challenges from video replay
- Key fields: `isOverturned`, `challengeTeamId`, `pX`/`pZ`, `strikeZoneTop`/`strikeZoneBottom`, original call code

**Baseball Savant Leaderboard** — `src/collect/savant.py`
- `https://baseballsavant.mlb.com/leaderboard/abs-challenges?csv=True`
- Four challenge types: `batter`, `batting-team`, `catcher`, `pitcher`
- Season-level aggregates; one snapshot pulled per day

## Database Schema

Three tables in `data/abs.duckdb`:
- `games` — one row per collected game (prevents re-fetching)
- `abs_challenges` — one row per challenged pitch with full pitch and review details
- `savant_leaderboard` — daily snapshots of Savant aggregated stats, partitioned by `pulled_date` and `challenge_type`

## Cron Setup

```
# Daily at 8am — run from project root
0 8 * * * cd /path/to/abs-pro && python src/jobs/daily.py >> logs/daily.log 2>&1
```

## Safety

- **Never drop or truncate tables** without explicit user confirmation — the DB is the source of truth and backfilling is time-consuming
- **Rate limit API calls** — add `time.sleep()` between requests when iterating over many games; the MLB Stats API and Baseball Savant are not metered but can throttle aggressive scrapers
- **R analysis scripts are the user's domain** — do not modify or generate analysis code unless explicitly asked
- **Confirm before any destructive operation** — deleting data, resetting the DB, or force-pushing git

## Notes

- All inserts use DuckDB's `ON CONFLICT DO NOTHING` — reruns are safe
- The `games` table acts as a collection log — a game present there won't be re-fetched
- pybaseball PR #506 adds `statcast_abs()` wrapping the Savant endpoint; once merged it can replace the manual CSV fetch in `savant.py`
