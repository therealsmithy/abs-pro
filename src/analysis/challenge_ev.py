"""
Compute expected run value (EV) for each ABS challenge.

For each challenge, looks up:
  re_current  — RE at the count/base-out state when the challenge was made
  re_flipped  — RE at the resulting state if the call is overturned
  re_delta    — re_flipped - re_current (runs gained by a successful challenge)

Count transitions:
  Called Strike overturned → Ball: (b, s) → (b+1, s-1), same base-out state
  Ball overturned → Strike: (b, s) → (b-1, s+1), same base-out state
    Special case: s=2, overturning to strike 3 records an out.
    Flipped state becomes (0, 0) with outs+1 (or RE=0 if it ends the inning).

Results are written back to abs_challenges as re_current, re_flipped, re_delta.
"""

import sys
from pathlib import Path
import pandas as pd

sys.path.insert(0, str(Path(__file__).parents[1] / "collect"))
from db import get_conn, init_db


def base_out_key(on_first, on_second, on_third, outs):
    bases = (
        ("1b" if on_first else "_")
        + ("2b" if on_second else "_")
        + ("3b" if on_third else "_")
    )
    return f"{bases}_{outs}out"


def walk_state(on_first, on_second, on_third, outs):
    """
    Returns (new_on_first, new_on_second, new_on_third, runs_scored) after a walk.
    Runners only advance when forced (1B occupied triggers the chain).
    """
    if on_first and on_second and on_third:
        return True, True, True, 1       # bases loaded walk — run scores
    elif on_first and on_second:
        return True, True, True, 0       # loads bases
    elif on_first and on_third:
        return True, True, True, 0       # loads bases (2B forced to 3B? no — 1B forced to 2B)
    elif on_first:
        return True, True, on_third, 0  # 1B forced to 2B
    else:
        return True, on_second, on_third, 0  # batter takes 1B, no force


def compute_ev():
    con = get_conn()

    re_rows = con.execute("SELECT balls, strikes, base_out, re FROM run_expectancy").fetchall()
    re = {(b, s, bo): r for b, s, bo, r in re_rows}

    df = con.execute("""
        SELECT game_pk, at_bat_index, pitch_index,
               balls, strikes, outs,
               on_first, on_second, on_third,
               original_call
        FROM abs_challenges
    """).df()

    print(f"Computing EV for {len(df):,} challenges...")

    re_current = []
    re_flipped = []

    for _, row in df.iterrows():
        b = int(row["balls"])
        s = int(row["strikes"])
        outs = int(row["outs"])
        bo = base_out_key(row["on_first"], row["on_second"], row["on_third"], outs)

        current = re.get((b, s, bo))
        re_current.append(current)

        call = row["original_call"] or ""

        if "Strike" in call:
            if b == 3:
                # Overturning strike on 3-ball count = walk
                f1, f2, f3, runs = walk_state(
                    row["on_first"], row["on_second"], row["on_third"], outs
                )
                new_bo = base_out_key(f1, f2, f3, outs)
                flipped = (re.get((0, 0, new_bo)) or 0.0) + runs
            else:
                # Normal: strike overturned to ball, count shifts
                flipped = re.get((b + 1, s - 1, bo))

        else:
            # Ball overturned to strike
            if s == 2:
                # Would be strike 3 — an out is recorded
                new_outs = outs + 1
                if new_outs >= 3:
                    flipped = 0.0
                else:
                    new_bo = base_out_key(row["on_first"], row["on_second"], row["on_third"], new_outs)
                    flipped = re.get((0, 0, new_bo))
            else:
                flipped = re.get((b - 1, s + 1, bo))

        re_flipped.append(flipped)

    df["re_current"] = re_current
    df["re_flipped"] = re_flipped
    df["re_delta"] = df["re_flipped"] - df["re_current"]

    # EV = P(overturn | call type) * re_delta
    # Using empirical overturn rates from our own data
    overturn_rates = con.execute("""
        SELECT original_call, AVG(is_overturned::int) as rate
        FROM abs_challenges GROUP BY 1
    """).df().set_index("original_call")["rate"].to_dict()

    df["p_overturn"] = df["original_call"].map(overturn_rates)
    df["ev"] = df["p_overturn"] * df["re_delta"]

    # Add columns to abs_challenges if not present
    for col, dtype in [
        ("re_current", "DOUBLE"), ("re_flipped", "DOUBLE"),
        ("re_delta", "DOUBLE"), ("p_overturn", "DOUBLE"), ("ev", "DOUBLE"),
    ]:
        try:
            con.execute(f"ALTER TABLE abs_challenges ADD COLUMN IF NOT EXISTS {col} {dtype}")
        except Exception:
            pass

    # Write back via a temp table
    con.execute("DROP TABLE IF EXISTS ev_updates")
    con.execute("""
        CREATE TEMP TABLE ev_updates (
            game_pk INTEGER, at_bat_index INTEGER, pitch_index INTEGER,
            re_current DOUBLE, re_flipped DOUBLE, re_delta DOUBLE,
            p_overturn DOUBLE, ev DOUBLE
        )
    """)
    updates = df[["game_pk", "at_bat_index", "pitch_index", "re_current", "re_flipped", "re_delta", "p_overturn", "ev"]]
    con.execute("INSERT INTO ev_updates SELECT game_pk, at_bat_index, pitch_index, re_current, re_flipped, re_delta, p_overturn, ev FROM updates")
    con.execute("""
        UPDATE abs_challenges
        SET re_current = ev_updates.re_current,
            re_flipped = ev_updates.re_flipped,
            re_delta   = ev_updates.re_delta,
            p_overturn = ev_updates.p_overturn,
            ev         = ev_updates.ev
        FROM ev_updates
        WHERE abs_challenges.game_pk       = ev_updates.game_pk
          AND abs_challenges.at_bat_index  = ev_updates.at_bat_index
          AND abs_challenges.pitch_index   = ev_updates.pitch_index
    """)

    nulls = df["re_delta"].isna().sum()
    print(f"Done. {nulls} challenges with no RE lookup.")

    print("\nTop 10 highest EV challenges (ev = p_overturn * re_delta):")
    top = df.dropna(subset=["ev"]).nlargest(10, "ev")[
        ["balls", "strikes", "outs", "on_first", "on_second", "on_third", "original_call", "re_delta", "p_overturn", "ev"]
    ]
    print(top.round(3).to_string(index=False))

    print("\nMean EV by original call:")
    print(df.groupby("original_call")[["re_delta", "ev"]].mean().round(3))

    con.close()


if __name__ == "__main__":
    init_db()
    compute_ev()
