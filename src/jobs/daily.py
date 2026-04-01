"""
Daily job: collect yesterday's GUMBO challenge data + refresh Savant leaderboard.
Run via cron: 0 8 * * * cd /path/to/abs-pro && python src/jobs/daily.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "collect"))

from gumbo import collect_yesterday
from savant import collect_leaderboard


def run():
    print("=== ABS Daily Collect ===")
    print("-- GUMBO challenges --")
    collect_yesterday()
    print("-- Savant leaderboard --")
    collect_leaderboard()
    print("=== Done ===")


if __name__ == "__main__":
    run()
