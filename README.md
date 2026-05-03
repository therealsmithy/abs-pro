# abs-pro

Collects and analyzes pitch-level data on MLB's 2026 ABS (Automated Ball-Strike) challenge system. Opening Day was March 26, 2026.

## Data Flow

```
MLB Stats API (GUMBO)              Baseball Savant
        │                                 │
  playByPlay endpoint             /leaderboard/abs-challenges
  one request per game             one request per challenge type
        │                                 │
  gumbo.py                         savant.py
  filters pitches with             fetches season-level
  reviewDetails (ABS only)         aggregates as CSV
        │                                 │
        └──────────────┬──────────────────┘
                       │
                 daily.py (cron, 8am)
                       │
               data/abs.duckdb
          ┌────────────┼────────────┐
        games    abs_challenges  savant_leaderboard
```

### GUMBO (`src/collect/gumbo.py`)

Pulls pitch-by-play data from `statsapi.mlb.com/api/v1/game/{gamePk}/playByPlay`. A pitch is an ABS challenge if it has a `reviewDetails` object with `reviewType: "MJ"`. Captured fields include pitch location (`pX`/`pZ`), strike zone boundaries, original call, whether the call was overturned, and which team challenged.

The `games` table acts as a collection log — a game already in that table is skipped, so reruns and backfills are safe.

### Savant (`src/collect/savant.py`)

Pulls the ABS leaderboard CSV for four challenge types: `batter`, `batting-team`, `catcher`, `pitcher`. This is a season-level aggregate (not pitch-level), snapshotted once per day so trends can be tracked over time.

### Daily job (`src/jobs/daily.py`)

Runs at 8am via cron. Collects yesterday's GUMBO data and refreshes the Savant leaderboard snapshot. All inserts use `ON CONFLICT DO NOTHING` — the job is safe to rerun.

```
0 8 * * * cd /Users/liamsmith/projects/abs-pro && python3 src/jobs/daily.py >> logs/daily.log 2>&1
```

## Database Schema (`data/abs.duckdb`)

**`games`** — one row per collected game  
**`abs_challenges`** — one row per challenged pitch; PK is `(game_pk, at_bat_index, pitch_index)`  
**`savant_leaderboard`** — daily leaderboard snapshots; PK is `(pulled_date, challenge_type, entity_name)`
