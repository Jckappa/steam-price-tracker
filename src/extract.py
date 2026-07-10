import csv
import json
import os
import time
from datetime import datetime, timezone

import requests

# ============================================================
# Constants
# ============================================================
DETAIL_URL = "https://store.steampowered.com/api/appdetails"
COUNTRIES = ["th", "us"]
GAMES_CSV = "config/games.csv"
OUTPUT_DIR = "data"
SLEEP_BETWEEN_CALLS = 2   # Seconds between requests; respects Steam rate limits
RETRIES = 3


# ============================================================
# 1. Load the list of games to track
# ============================================================
def load_games(csv_path):
    """Read the tracked-games config and return rows we need downstream.

    Args:
        csv_path: Path to config/games.csv.

    Returns:
        list[dict]: One dict per game with 'steam_appid' (int) and 'game_name'.

    Raises:
        FileNotFoundError: If the config file is missing. We let this propagate
            on purpose -- no config means there is nothing to do.
    """
    games = []
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # CSV values are always strings. steam_appid must be INT64 in our
            # schema, so we cast it here at the boundary.
            games.append({
                "steam_appid": int(row["steam_appid"]),
                "game_name": row["game_name"],
            })
    return games


# ============================================================
# 2. Fetch price for one game in one region  (the heart)
# ============================================================
def fetch_price(appid, country_code, snapshot_date, ingested_at, retries=RETRIES):
    """Fetch a single price snapshot. ALWAYS returns a full row dict.

    Every exit path -- success, success=false, network death, bad JSON --
    returns a dict with all 11 columns so that failures are recorded, not
    silently dropped.

    Args:
        appid: Steam application id (int).
        country_code: Region code we request, e.g. "th" or "us".
        snapshot_date: date object, frozen once per run.
        ingested_at: datetime object, frozen once per run.

    Returns:
        dict: A row matching the raw_price_snapshots schema.
    """
    # Base row: the values we already know before the API is even called.
    # Everything price-related defaults to None and is filled in on success.
    row = {
        "steam_appid": appid,
        "game_name": None,
        "country_code": country_code,
        "snapshot_date": snapshot_date.isoformat(),
        "is_free": None,
        "initial_price": None,
        "final_price": None,
        "discount_percent": None,
        "currency": None,
        "api_success": False,
        "ingested_at": ingested_at.isoformat(),
    }

    for attempt in range(retries):
        try:
            response = requests.get(
                DETAIL_URL,
                params={"appids": appid, "cc": country_code, "l": "en"},
                timeout=10,
            )
            response.raise_for_status()

            # Gotcha: the top-level key is the appid as a STRING.
            payload = response.json()[str(appid)]

            # ---- Business logic: use if, never retry ----
            # success=false is a stable answer; retrying returns false forever.
            if not payload.get("success"):
                row["api_success"] = False
                return row

            data = payload["data"]
            row["api_success"] = True
            row["game_name"] = data.get("name")
            row["is_free"] = data.get("is_free")

            # price_overview is absent for free / delisted / region-locked games.
            price = data.get("price_overview")
            if price:
                row["initial_price"] = price.get("initial")
                row["final_price"] = price.get("final")
                row["discount_percent"] = price.get("discount_percent")
                row["currency"] = price.get("currency")

            return row

        # ---- Network problems: retry with exponential backoff ----
        except requests.RequestException as e:
            wait = 2 ** attempt   # 1s, 2s, 4s
            print(f"  {appid}/{country_code} attempt {attempt + 1} failed: "
                  f"{e} -> retry in {wait}s")
            time.sleep(wait)

        # ---- Data-shape problems: catch, do NOT crash the whole run ----
        # KeyError: appid key missing. ValueError: response wasn't valid JSON.
        except (KeyError, ValueError) as e:
            print(f"  {appid}/{country_code} data error: {e}")
            row["api_success"] = False
            return row

    # All network retries exhausted.
    print(f"  {appid}/{country_code} failed permanently after {retries} tries")
    row["api_success"] = False
    return row


# ============================================================
# 3. Write records to a JSONL file
# ============================================================
def write_jsonl(records, output_path):
    """Write records as JSON Lines (one JSON object per line).

    Args:
        records: list of row dicts.
        output_path: Destination path.

    Returns:
        int: Number of rows written.
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        for record in records:
            # ensure_ascii=False keeps Thai/Japanese game titles readable.
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    return len(records)


# ============================================================
# 4. Orchestration
# ============================================================
def main():
    """Fetch price snapshots for every game x country and write one JSONL file."""
    # Freeze time ONCE, outside the loop, so every row in this batch shares the
    # same snapshot_date even if the run crosses midnight.
    ingested_at = datetime.now(timezone.utc)
    snapshot_date = ingested_at.date()

    games = load_games(GAMES_CSV)
    print(f"Loaded {len(games)} games from {GAMES_CSV}")

    records = []
    for i, game in enumerate(games,start=1):
        for country in COUNTRIES:
            print(f"[{i}/{len(games)}] {game['steam_appid']} ({country})")
            row = fetch_price(
                game["steam_appid"], country, snapshot_date, ingested_at
            )
            records.append(row)
            time.sleep(SLEEP_BETWEEN_CALLS)

    output_path = f"{OUTPUT_DIR}/snapshots_{snapshot_date.isoformat()}.jsonl"
    n = write_jsonl(records, output_path)

    success = sum(1 for r in records if r["api_success"])
    print(f"\nDone. Wrote {n} rows to {output_path}")
    print(f"  success={success}  failed={n - success}")


if __name__ == "__main__":
    main()