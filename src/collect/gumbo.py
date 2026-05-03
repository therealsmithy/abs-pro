"""
Collect pitch-level ABS challenge events from the MLB Stats API (GUMBO).
"""

import requests
import pandas as pd
from datetime import date, timedelta
from db import get_conn, init_db

BASE = "https://statsapi.mlb.com/api/v1"


def get_game_pks(start: str, end: str) -> list[dict]:
    """Return list of {gamePk, gameDate, homeTeam, awayTeam} for date range."""
    url = f"{BASE}/schedule"
    params = {
        "sportId": 1,
        "startDate": start,
        "endDate": end,
        "gameType": "R",
        "hydrate": "team",
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    games = []
    for day in r.json().get("dates", []):
        for g in day.get("games", []):
            games.append({
                "game_pk": g["gamePk"],
                "game_date": day["date"],
                "home_team_id": g["teams"]["home"]["team"]["id"],
                "away_team_id": g["teams"]["away"]["team"]["id"],
                "home_team": g["teams"]["home"]["team"]["name"],
                "away_team": g["teams"]["away"]["team"]["name"],
            })
    return games


def get_challenges(game_pk: int) -> list[dict]:
    """Return all ABS challenge pitches from a single game."""
    url = f"{BASE}/game/{game_pk}/playByPlay"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()

    rows = []
    away_score = 0
    home_score = 0

    for play in data.get("allPlays", []):
        ab_index = play["atBatIndex"]
        inning = play["about"]["inning"]
        half = play["about"]["halfInning"]
        batter = play["matchup"]["batter"]
        pitcher = play["matchup"]["pitcher"]
        bat_side = play["matchup"].get("batSide", {}).get("code")
        pitch_hand = play["matchup"].get("pitchHand", {}).get("code")

        # Base runners at start of at-bat (runners with a non-null start base)
        runner_starts = {
            r["movement"]["start"]
            for r in play.get("runners", [])
            if r.get("movement", {}).get("start")
        }
        on_first  = "1B" in runner_starts
        on_second = "2B" in runner_starts
        on_third  = "3B" in runner_starts

        # Score at start of this at-bat
        ab_away_score = away_score
        ab_home_score = home_score

        for event in play.get("playEvents", []):
            if event.get("type") != "pitch":
                continue
            review = event.get("reviewDetails")
            if not review or review.get("reviewType") != "MJ":
                continue

            count = event.get("count", {})
            pd_ = event.get("pitchData", {})
            coords = pd_.get("coordinates", {})
            rows.append({
                "game_pk": game_pk,
                "at_bat_index": ab_index,
                "pitch_index": event.get("index"),
                "inning": inning,
                "half_inning": half,
                "batter_id": batter.get("id"),
                "batter_name": batter.get("fullName"),
                "pitcher_id": pitcher.get("id"),
                "pitcher_name": pitcher.get("fullName"),
                "bat_side": bat_side,
                "pitch_hand": pitch_hand,
                "balls": count.get("balls"),
                "strikes": count.get("strikes"),
                "outs": count.get("outs"),
                "away_score": ab_away_score,
                "home_score": ab_home_score,
                "on_first": on_first,
                "on_second": on_second,
                "on_third": on_third,
                "challenging_team": review.get("challengeTeamId"),
                "challenger_id": review.get("player", {}).get("id"),
                "challenger_name": review.get("player", {}).get("fullName"),
                "original_call": event.get("details", {}).get("call", {}).get("description"),
                "is_overturned": review.get("isOverturned"),
                "px": coords.get("pX"),
                "pz": coords.get("pZ"),
                "sz_top": pd_.get("strikeZoneTop"),
                "sz_bot": pd_.get("strikeZoneBottom"),
                "zone": pd_.get("zone"),
                "pitch_type": event.get("details", {}).get("type", {}).get("description"),
            })

        result = play.get("result", {})
        away_score = result.get("awayScore", away_score)
        home_score = result.get("homeScore", home_score)

    return rows


def collect_range(start: str, end: str):
    con = get_conn()
    already = {r[0] for r in con.execute("SELECT game_pk FROM games").fetchall()}

    games = get_game_pks(start, end)
    new_games = [g for g in games if g["game_pk"] not in already]
    print(f"Found {len(games)} games, {len(new_games)} new.")

    all_challenges = []
    for g in new_games:
        pk = g["game_pk"]
        try:
            challenges = get_challenges(pk)
            for c in challenges:
                c["game_date"] = g["game_date"]
            all_challenges.extend(challenges)
            # Mark game collected even if no challenges (so we don't re-fetch)
            con.execute(
                "INSERT INTO games VALUES (?,?,?,?,?,?,current_timestamp) ON CONFLICT DO NOTHING",
                [pk, g["game_date"], g["home_team_id"], g["away_team_id"],
                 g["home_team"], g["away_team"]]
            )
            print(f"  {pk} ({g['game_date']}): {len(challenges)} challenges")
        except Exception as e:
            print(f"  {pk} ERROR: {e}")

    if all_challenges:
        col_order = [
            "game_pk", "at_bat_index", "pitch_index", "game_date",
            "inning", "half_inning",
            "batter_id", "batter_name", "pitcher_id", "pitcher_name",
            "bat_side", "pitch_hand",
            "balls", "strikes", "outs",
            "away_score", "home_score",
            "on_first", "on_second", "on_third",
            "challenging_team", "challenger_id", "challenger_name",
            "original_call", "is_overturned",
            "px", "pz", "sz_top", "sz_bot", "zone", "pitch_type",
        ]
        df = pd.DataFrame(all_challenges)[col_order]
        cols = ", ".join(col_order)
        con.execute(f"""
            INSERT INTO abs_challenges ({cols})
            SELECT {cols} FROM df
            ON CONFLICT DO NOTHING
        """)
        print(f"Inserted {len(df)} challenge rows.")

    con.close()


def collect_since_opening_day():
    """Collect all 2026 regular season data from Opening Day to today."""
    collect_range("2026-03-26", date.today().isoformat())


def collect_yesterday():
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    collect_range(yesterday, yesterday)


if __name__ == "__main__":
    import sys
    init_db()
    if len(sys.argv) == 3:
        collect_range(sys.argv[1], sys.argv[2])
    else:
        collect_since_opening_day()
