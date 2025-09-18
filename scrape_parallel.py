import json
import os
from multiprocessing import Pool, cpu_count
from fantasy_data_scrape import (
    get_players_for_year,
    scrape_player,
    get_table_metadata_for_positions,
    START_YEAR,
    END_YEAR,
)

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)


def scrape_wrapper(args):
    """Wrapper for multiprocessing scrape calls."""
    pid, name, url, metadata_map = args
    try:
        pdata = scrape_player(pid, name, url, metadata_map)
        if pdata:
            filepath = os.path.join(DATA_DIR, f"{pid}.json")
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(pdata, f, indent=2)
            print(f"[OK] Saved {name} ({pid}) → {filepath}")
        return pdata
    except Exception as e:
        print(f"[ERROR] Failed {name} ({pid}): {e}")
        return None


def main():
    # Step 1: Build universal metadata
    print("=== Building universal metadata ===")
    metadata_map = get_table_metadata_for_positions(START_YEAR)

    # Step 2: Gather all players across years
    all_players = []
    for year in range(START_YEAR, END_YEAR + 1):
        print(f"=== Collecting players for {year} ===")
        players = get_players_for_year(year)
        # players = [(pid, name, team, url)]
        for pid, name, _, url in players:
            all_players.append((pid, name, url, metadata_map))

    print(f"Total players to scrape: {len(all_players)}")

    # Step 3: Parallel scrape
    workers = min(cpu_count(), 8)  # cap at 8 to avoid hammering site
    print(f"=== Starting scrape with {workers} workers ===")

    with Pool(processes=workers) as pool:
        pool.map(scrape_wrapper, all_players)


if __name__ == "__main__":
    main()
