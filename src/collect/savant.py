"""
Collect aggregated ABS leaderboard data from Baseball Savant.
"""

import io
import requests
import pandas as pd
from datetime import date
from db import get_conn, init_db

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Referer": "https://baseballsavant.mlb.com/",
}

LEADERBOARD_URL = "https://baseballsavant.mlb.com/leaderboard/abs-challenges"

CHALLENGE_TYPES = ["batter", "batting-team", "catcher", "pitcher"]

KEEP_COLS = [
    "entity_name",
    "team_abbr",
    "n_challenges",
    "n_overturns",
    "n_confirms",
    "rate_overturns",
    "total_vs_expected",
    "net_for",
    "net_against",
    "n_strikeouts_flip",
    "n_walks_flip",
    "n_challenges_against",
    "n_overturns_against",
    "rate_overturns_against",
]


def fetch_leaderboard(challenge_type: str, year: int = 2026) -> pd.DataFrame:
    params = {
        "challengeType": challenge_type,
        "level": "mlb",
        "gameType": "regular",
        "year": year,
        "csv": "True",
    }
    query = "&".join(f"{k}={v}" for k, v in params.items())
    url = f"{LEADERBOARD_URL}?{query}"
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text))
    return df


def collect_leaderboard(year: int = 2026):
    con = get_conn()
    today = date.today().isoformat()

    for ctype in CHALLENGE_TYPES:
        try:
            df = fetch_leaderboard(ctype, year)
            # Normalize column names
            df.columns = [c.lower().strip() for c in df.columns]

            present = [c for c in KEEP_COLS if c in df.columns]
            df = df[present].copy()
            df["pulled_date"] = today
            df["challenge_type"] = ctype

            con.execute("""
                INSERT INTO savant_leaderboard
                SELECT pulled_date, challenge_type, entity_name, team_abbr,
                       n_challenges, n_overturns, n_confirms, rate_overturns,
                       total_vs_expected, net_for, net_against,
                       n_strikeouts_flip, n_walks_flip,
                       n_challenges_against, n_overturns_against, rate_overturns_against
                FROM df
                ON CONFLICT DO NOTHING
            """)
            print(f"  {ctype}: {len(df)} rows")
        except Exception as e:
            print(f"  {ctype} ERROR: {e}")

    con.close()


if __name__ == "__main__":
    init_db()
    collect_leaderboard()
