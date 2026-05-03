"""
Pull pitch-by-pitch Statcast data used to calculate run expectancy.
Pulls regular season pitches only; skips dates already in the DB.
"""

import pybaseball
import pandas as pd
from datetime import date, timedelta
from db import get_conn, init_db

KEEP_COLS = [
    "game_pk", "game_date", "inning", "inning_topbot",
    "at_bat_number", "pitch_number",
    "balls", "strikes", "outs_when_up",
    "on_1b", "on_2b", "on_3b",
    "bat_score", "post_bat_score",
    "type", "events", "description",
]


def collect_statcast(start: str, end: str):
    con = get_conn()

    existing = {
        r[0].isoformat()
        for r in con.execute("SELECT DISTINCT game_date FROM statcast_pitches").fetchall()
    }

    print(f"Pulling Statcast {start} to {end}...")
    df = pybaseball.statcast(start, end, verbose=True)

    if df is None or df.empty:
        print("No data returned.")
        con.close()
        return

    df = df[df["game_type"] == "R"].copy()
    df = df[KEEP_COLS].copy()

    for col in ["on_1b", "on_2b", "on_3b"]:
        df[col] = df[col].notna()

    df["game_date"] = pd.to_datetime(df["game_date"]).dt.date
    df = df[~df["game_date"].astype(str).isin(existing)]

    if df.empty:
        print("All dates already collected.")
        con.close()
        return

    cols = ", ".join(KEEP_COLS)
    con.execute(f"""
        INSERT INTO statcast_pitches ({cols})
        SELECT {cols} FROM df
        ON CONFLICT DO NOTHING
    """)
    print(f"Inserted {len(df)} pitch rows.")
    con.close()


def collect_since_opening_day():
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    collect_statcast("2026-03-26", yesterday)


if __name__ == "__main__":
    init_db()
    collect_since_opening_day()
