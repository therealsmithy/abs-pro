import duckdb
from pathlib import Path

DB_PATH = Path(__file__).parents[2] / "data" / "abs.duckdb"


def get_conn():
    return duckdb.connect(str(DB_PATH))


def init_db():
    con = get_conn()

    con.execute("""
        CREATE TABLE IF NOT EXISTS games (
            game_pk       INTEGER PRIMARY KEY,
            game_date     DATE,
            home_team_id  INTEGER,
            away_team_id  INTEGER,
            home_team     VARCHAR,
            away_team     VARCHAR,
            collected_at  TIMESTAMP DEFAULT current_timestamp
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS abs_challenges (
            game_pk           INTEGER,
            at_bat_index      INTEGER,
            pitch_index       INTEGER,
            game_date         DATE,
            inning            INTEGER,
            half_inning       VARCHAR,
            batter_id         INTEGER,
            batter_name       VARCHAR,
            pitcher_id        INTEGER,
            pitcher_name      VARCHAR,
            bat_side          VARCHAR,
            pitch_hand        VARCHAR,
            balls             INTEGER,
            strikes           INTEGER,
            outs              INTEGER,
            away_score        INTEGER,
            home_score        INTEGER,
            on_first          BOOLEAN,
            on_second         BOOLEAN,
            on_third          BOOLEAN,
            challenging_team  INTEGER,
            challenger_id     INTEGER,
            challenger_name   VARCHAR,
            original_call     VARCHAR,
            is_overturned     BOOLEAN,
            px                DOUBLE,
            pz                DOUBLE,
            sz_top            DOUBLE,
            sz_bot            DOUBLE,
            zone              INTEGER,
            pitch_type        VARCHAR,
            PRIMARY KEY (game_pk, at_bat_index, pitch_index)
        )
    """)

    # Migrate existing databases
    new_cols = [
        ("bat_side",   "VARCHAR"),
        ("pitch_hand", "VARCHAR"),
        ("balls",      "INTEGER"),
        ("strikes",    "INTEGER"),
        ("outs",       "INTEGER"),
        ("away_score", "INTEGER"),
        ("home_score", "INTEGER"),
        ("on_first",   "BOOLEAN"),
        ("on_second",  "BOOLEAN"),
        ("on_third",   "BOOLEAN"),
    ]
    for col, dtype in new_cols:
        try:
            con.execute(f"ALTER TABLE abs_challenges ADD COLUMN IF NOT EXISTS {col} {dtype}")
        except Exception:
            pass

    con.execute("""
        CREATE TABLE IF NOT EXISTS statcast_pitches (
            game_pk         INTEGER,
            game_date       DATE,
            inning          INTEGER,
            inning_topbot   VARCHAR,
            at_bat_number   INTEGER,
            pitch_number    INTEGER,
            balls           INTEGER,
            strikes         INTEGER,
            outs_when_up    INTEGER,
            on_1b           BOOLEAN,
            on_2b           BOOLEAN,
            on_3b           BOOLEAN,
            bat_score       INTEGER,
            post_bat_score  INTEGER,
            type            VARCHAR,
            events          VARCHAR,
            description     VARCHAR,
            PRIMARY KEY (game_pk, at_bat_number, pitch_number)
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS savant_leaderboard (
            pulled_date               DATE,
            challenge_type            VARCHAR,
            entity_name               VARCHAR,
            team_abbr                 VARCHAR,
            n_challenges              INTEGER,
            n_overturns               INTEGER,
            n_confirms                INTEGER,
            rate_overturns            DOUBLE,
            total_vs_expected         DOUBLE,
            net_for                   DOUBLE,
            net_against               DOUBLE,
            n_strikeouts_flip         INTEGER,
            n_walks_flip              INTEGER,
            n_challenges_against      INTEGER,
            n_overturns_against       INTEGER,
            rate_overturns_against    DOUBLE,
            PRIMARY KEY (pulled_date, challenge_type, entity_name)
        )
    """)

    con.close()
    print(f"DB initialized at {DB_PATH}")


if __name__ == "__main__":
    init_db()
