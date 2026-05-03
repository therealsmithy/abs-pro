"""
Calculate run expectancy matrix from statcast_pitches.

For each pitch, computes runs scored from that point to end of half-inning,
then averages by count (balls x strikes) and base-out state (24 states).
Writes results to the run_expectancy table in abs.duckdb.
"""

import sys
from pathlib import Path
import pandas as pd

sys.path.insert(0, str(Path(__file__).parents[1] / "collect"))
from db import get_conn, init_db


def build_re_matrix():
    con = get_conn()

    print("Loading pitches...")
    df = con.execute("""
        SELECT
            game_pk,
            inning,
            inning_topbot,
            at_bat_number,
            pitch_number,
            balls,
            strikes,
            outs_when_up,
            on_1b,
            on_2b,
            on_3b,
            bat_score,
            post_bat_score
        FROM statcast_pitches
        WHERE type IS NOT NULL
        ORDER BY game_pk, inning, inning_topbot, at_bat_number, pitch_number
    """).df()

    print(f"  {len(df):,} pitches loaded.")

    # Max runs scored by batting team per half-inning
    inning_max = (
        df.groupby(["game_pk", "inning", "inning_topbot"])["post_bat_score"]
        .max()
        .reset_index()
        .rename(columns={"post_bat_score": "inning_final_score"})
    )

    df = df.merge(inning_max, on=["game_pk", "inning", "inning_topbot"])

    # Runs scored from this pitch to end of half-inning
    df["runs_to_end"] = df["inning_final_score"] - df["bat_score"]

    # Base-out state label
    def base_out_state(row):
        bases = (
            ("1b" if row["on_1b"] else "_")
            + ("2b" if row["on_2b"] else "_")
            + ("3b" if row["on_3b"] else "_")
        )
        return f"{bases}_{row['outs_when_up']}out"

    print("Computing base-out states...")
    df["base_out"] = df.apply(base_out_state, axis=1)

    # Filter to valid counts (0-3 balls, 0-2 strikes)
    df = df[(df["balls"] <= 3) & (df["strikes"] <= 2)]

    print("Building RE matrix...")
    re = (
        df.groupby(["balls", "strikes", "base_out"])["runs_to_end"]
        .agg(["mean", "count"])
        .reset_index()
        .rename(columns={"mean": "re", "count": "n"})
    )

    re["re"] = re["re"].round(3)

    # Write to DB
    con.execute("""
        CREATE TABLE IF NOT EXISTS run_expectancy (
            balls       INTEGER,
            strikes     INTEGER,
            base_out    VARCHAR,
            re          DOUBLE,
            n           INTEGER,
            PRIMARY KEY (balls, strikes, base_out)
        )
    """)
    con.execute("DELETE FROM run_expectancy")
    con.execute("INSERT INTO run_expectancy SELECT balls, strikes, base_out, re, n FROM re")

    total = len(re)
    print(f"Written {total} RE cells to run_expectancy table.")

    # Print a quick readable summary (base-out state vs count)
    print("\nSample — bases empty:")
    sample = re[re["base_out"] == "____0out"].sort_values(["balls", "strikes"])
    for _, row in sample.iterrows():
        print(f"  {int(row['balls'])}-{int(row['strikes'])}  RE={row['re']:.3f}  (n={int(row['n']):,})")

    con.close()


if __name__ == "__main__":
    init_db()
    build_re_matrix()
