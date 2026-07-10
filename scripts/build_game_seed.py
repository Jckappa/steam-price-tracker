import csv
import time
import requests

# ============================================================
# Section 1: Constants
# ============================================================
# UPPER_CASE is the Python convention for values that never change at runtime.
CHART_URL = "https://api.steampowered.com/ISteamChartsService/GetMostPlayedGames/v1/"
DETAIL_URL = "https://store.steampowered.com/api/appdetails"

TARGET_PAID_GAMES = 50      # How many paid games we want in the final seed file
CANDIDATE_POOL = 150        # Fetch extra appids: free games will be filtered out
SLEEP_BETWEEN_CALLS = 2   # Seconds. Respects the informal ~200 req / 5 min limit
OUTPUT_PATH = "config/games.csv"

# Steam classifies some software as type="game" (e.g. Wallpaper Engine,
# Crosshair X). Filter them out by genre: real games never carry these.
NON_GAME_GENRES = {
    "Utilities",
    "Design & Illustration",
    "Animation & Modeling",
    "Video Production",
    "Audio Production",
    "Photo Editing",
    "Web Publishing",
    "Software Training",
    "Game Development",
    "Education",
}

CSV_COLUMNS = [
    "steam_appid", "game_name", "category",
    "developer", "publisher", "release_date",
    "platforms", "genres", "steam_categories",
]


# ============================================================
# Section 2: Fetch appids from the most-played chart
# ============================================================
def fetch_top_appids(limit=CANDIDATE_POOL):
    """Fetch the most-played games chart and return a list of appids.

    Response shape:
        {"response": {"ranks": [{"rank": 1, "appid": 730, ...}, ...]}}

    Args:
        limit: Maximum number of appids to return.

    Returns:
        list[int]: Steam appids ordered by current player count.
    """
    r = requests.get(CHART_URL, timeout=10)
    r.raise_for_status()  # Raise immediately on any 4xx/5xx response

    ranks = r.json()["response"]["ranks"]

    # List comprehension: build a new list in a single expression.
    # [:limit] slices off only the first `limit` entries.
    return [item["appid"] for item in ranks[:limit]]


# ============================================================
# Section 3: Fetch details for a single game
# ============================================================
def fetch_game_detail(appid, retries=3):
    """Fetch metadata for one appid from the Steam Storefront API.

    Args:
        appid: The Steam application id to look up.
        retries: How many times to retry on transient network failures.

    Returns:
        dict: A CSV-ready row, or None if the app should be skipped
              (free game, not a game, no data, or permanent failure).
    """
    for attempt in range(retries):  # attempt = 0, 1, 2
        try:
            r = requests.get(
                DETAIL_URL,
                params={"appids": appid, "cc": "th", "l": "en"},
                timeout=10,
            )
            r.raise_for_status()

            # Gotcha: the top-level key is the appid as a STRING, not an int.
            payload = r.json()[str(appid)]

            # ---- Business logic: use `if`, never retry ----
            # success=false means Steam has no data for this app.
            # Retrying would return false forever, so bail out immediately.
            if not payload.get("success"):
                print(f"  Skip {appid}: success=false")
                return None

            data = payload["data"]

            # The chart mixes in DLC, soundtracks, and tools. Keep games only.
            if data.get("type") != "game":
                print(f"  Skip {appid}: type={data.get('type')}")
                return None

            # Project scope: paid games only.
            if data.get("is_free"):
                print(f"  Skip {appid} ({data.get('name')}): free game")
                return None
           
           # Unreleased games have no purchase price yet. is_free=false only means
            # "not free" -- it does NOT guarantee a price exists.
            if data.get("release_date", {}).get("coming_soon"):
                print(f"  Skip {appid} ({data.get('name')}): unreleased")
                return None

            # The real contract this pipeline depends on: a price we can track daily.
            # Missing price_overview also catches delisted or region-locked titles.
            if "price_overview" not in data:
                print(f"  Skip {appid} ({data.get('name')}): no price data for cc=th")
                return None
           
            # ---- Flatten nested structures so they fit in a CSV cell ----
            # Pipe delimiter, never comma: a comma would break the CSV format.
             # Extract genre names once, then use them for both filtering and output.
            genre_names = [g["description"] for g in data.get("genres", [])]

            # Steam labels some utilities as type="game", so genre is the real signal.
            # set() & set() returns the overlap; a non-empty overlap means it's software.
            if set(genre_names) & NON_GAME_GENRES:
                print(f"  Skip {appid} ({data.get('name')}): non-game genre {genre_names}")
                return None

            genres = "|".join(genre_names)
            categories = "|".join(c["description"] for c in data.get("categories", []))

            # developers/publishers are already lists of strings.
            developer = "|".join(data.get("developers", []))
            publisher = "|".join(data.get("publishers", []))

            # platforms is a dict: {"windows": true, "mac": false, "linux": false}
            # Keep only the keys whose value is True -> "windows|mac"
            platforms = "|".join(k for k, v in data.get("platforms", {}).items() if v)

            # release_date is nested: {"coming_soon": false, "date": "25 Feb, 2022"}
            release_date = data.get("release_date", {}).get("date", "")

            return {
                "steam_appid": appid,
                "game_name": data["name"],
                "category": "trending",  # Sourced from the chart; personal picks added manually
                "developer": developer,
                "publisher": publisher,
                "release_date": release_date,
                "platforms": platforms,
                "genres": genres,
                "steam_categories": categories,
            }

        # ---- Network problems: retry with exponential backoff ----
        # RequestException is the base class for timeouts, connection errors,
        # and the HTTPError raised by raise_for_status().
        except requests.RequestException as e:
            wait = 2 ** attempt  # Back off 1s, then 2s, then 4s
            print(f"  {appid} attempt {attempt + 1} failed: {e} -> retrying in {wait}s")
            time.sleep(wait)

    print(f"  {appid} failed permanently after {retries} attempts, skipping")
    return None


# ============================================================
# Section 4: Orchestration
# ============================================================
def main():
    """Build config/games.csv from the current most-played chart."""
    appids = fetch_top_appids()
    print(f"Fetched {len(appids)} appids from the chart")

    rows = []
    for appid in appids:
        # Stop early once the quota is met; don't waste API calls.
        if len(rows) >= TARGET_PAID_GAMES:
            break

        detail = fetch_game_detail(appid)
        if detail is not None:
            rows.append(detail)
            print(f"  Kept [{len(rows)}/{TARGET_PAID_GAMES}] {detail['game_name']}")

        time.sleep(SLEEP_BETWEEN_CALLS)

    # ---- Write the seed file ----
    # newline="" prevents blank rows between records on Windows.
    # encoding="utf-8" keeps non-Latin game titles from being mangled.
    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nDone. Wrote {len(rows)} games to {OUTPUT_PATH}")


# Only run main() when this file is executed directly, not when imported.
if __name__ == "__main__":
    main()