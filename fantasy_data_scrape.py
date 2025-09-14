import requests
from bs4 import BeautifulSoup
import json
import time
import os
from collections import defaultdict

BASE_URL = "https://www.pro-football-reference.com"
START_YEAR, END_YEAR = 2010, 2024

# HOW TO CONSTRUCT PLAYER PAGE URL:
# https://www.pro-football-reference.com/players/{first_letter_of_last_name}/{player_id}/gamelog/{year}

seen_players = set()
DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

METADATA_FILE = "data/table_metadata.txt"

def get_table_metadata(url, filename=METADATA_FILE):
    """Extract table metadata (headers + categories + tooltips) from a player's game log page."""
    soup = fetch_soup(url)
    table = soup.find("table", {"id": "stats"})
    if not table:
        print(f"[WARN] No stats table found at {url}")
        return {}

    headers = []
    categories = []
    tips = []

    thead = table.find("thead")
    if thead:
        rows = thead.find_all("tr")
        if len(rows) >= 2:
            top = rows[0].find_all("th")
            bottom = rows[1].find_all("th")

            # Map each bottom col to its category by colspan
            cat_map = []
            for th in top:
                colspan = int(th.get("colspan", 1))
                cat = th.get_text(strip=True)
                cat_map.extend([cat] * colspan)

            for i, th in enumerate(bottom):
                col = th.get_text(strip=True)
                cat = cat_map[i] if i < len(cat_map) else ""
                tip = th.get("data-tip", "").strip()
                headers.append(col)
                categories.append(cat)
                tips.append(tip)

    # Build map (category, column) → unique name
    name_count = {}
    meta = {}
    for cat, col, tip in zip(categories, headers, tips):
        key = (cat, col)
        base = col
        if base in name_count:
            name_count[base] += 1
            unique = f"{cat[:2].lower()}{base}"
        else:
            name_count[base] = 1
            unique = base
        meta[key] = {"unique": unique, "tip": tip}

    # Write to file if filename provided
    if filename:
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, "w", encoding="utf-8") as f:
            for (cat, col), info in meta.items():
                unique = info["unique"]
                tip = info["tip"]
                # Format: category,col:unique | tip
                f.write(f"{cat},{col}:{unique} | {tip}\n")
        print(f"[INFO] Metadata written to {filename} with {len(meta)} headers")

    return meta

def get_table_metadata_for_positions(year, positions=("QB", "RB", "WR", "TE"), filename=METADATA_FILE):
    """
    Collect metadata across multiple positions to build a universal schema.
    Looks up one player of each position (if available) from the fantasy stats page.
    """
    url = f"{BASE_URL}/years/{year}/fantasy.htm"
    soup = fetch_soup(url)
    table = soup.find("table", {"id": "fantasy"})
    if not table:
        print(f"[WARN] No fantasy table for year {year}")
        return {}

    found = {}
    for row in table.tbody.find_all("tr"):
        if "class" in row.attrs and "thead" in row["class"]:
            continue
        pos_cell = row.find("td", {"data-stat": "fantasy_pos"})
        name_cell = row.find("td", {"data-stat": "player"})
        if not pos_cell or not name_cell or not name_cell.a:
            continue
        pos = pos_cell.get_text(strip=True)
        if pos in positions and pos not in found:
            player_link = BASE_URL + name_cell.a["href"]
            # Use first available gamelog link
            player_soup = fetch_soup(player_link)
            nav = player_soup.find("div", {"id": "inner_nav"})
            if not nav:
                continue
            game_log_header = nav.find("span", string="Game Logs")
            if not game_log_header:
                continue
            ul = game_log_header.find_next("ul")
            if not ul:
                continue
            first_link = ul.find("a", href=True)
            if not first_link:
                continue
            gamelog_url = BASE_URL + first_link["href"]
            print(f"[INFO] Fetching metadata from {pos} {name_cell.a.text} → {gamelog_url}")
            meta = get_table_metadata(gamelog_url, filename=None)  # don’t overwrite file yet
            found[pos] = meta
            if len(found) == len(positions):
                break

    # Merge all metadata
    merged = {}
    for m in found.values():
        merged.update(m)

    # Save to file
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, "w", encoding="utf-8") as f:
        for (cat, col), info in meta.items():
            unique = info["unique"]
            tip = info["tip"]
            f.write(f"{cat},{col}:{unique} | {tip}\n")


    print(f"[INFO] Universal metadata saved with {len(merged)} headers")
    return merged

def fetch_soup(url):
    res = requests.get(url)
    res.raise_for_status()
    return BeautifulSoup(res.text, "html.parser")

def get_players_for_year(year):
    """Get all players listed on the fantasy stats page for a given year."""
    url = f"{BASE_URL}/years/{year}/fantasy.htm"
    soup = fetch_soup(url)
    table = soup.find("table", {"id": "fantasy"})
    players = []
    for row in table.tbody.find_all("tr"):
        if "class" in row.attrs and "thead" in row["class"]:
            continue
        name_cell = row.find("td", {"data-stat": "player"})
        if not name_cell or not name_cell.a:
            continue
        player_name = name_cell.a.text.strip()
        player_id = name_cell["data-append-csv"]
        player_link = BASE_URL + name_cell.a["href"]
        players.append((player_id, player_name, player_link))
    print(f"Found {len(players)} players for {year}")
    return players

def scrape_player(player_id, player_name, player_url, metadata_map):
    """Scrape all available game logs for one player across years."""
    if player_id in seen_players:
        return None
    seen_players.add(player_id)

    data = {"name": player_name, "id": player_id, "years": {}}
    soup = fetch_soup(player_url)

    nav = soup.find("div", {"id": "inner_nav"})
    if not nav:
        print(f"  No nav found for {player_name}")
        return data

    game_log_header = nav.find("span", string="Game Logs")
    if not game_log_header:
        print(f"  No game log section for {player_name}")
        return data

    ul = game_log_header.find_next("ul")
    if not ul:
        print(f"  No game log list for {player_name}")
        return data

    # Collect year → url map once
    year_links = {}
    for link in ul.find_all("a", href=True):
        if "/gamelog/" not in link["href"]:
            continue
        year = link.text.strip()
        if not year.isdigit():
            continue
        year = int(year)
        if year < START_YEAR or year > END_YEAR:
            continue
        year_links[year] = BASE_URL + link["href"]

    # Process each year
    for year, year_url in sorted(year_links.items()):
        print(f"  Scraping {year} game log for {player_name}...")
        year_data = scrape_gamelog(year_url, metadata_map)
        if year_data:
            data["years"][str(year)] = year_data
        time.sleep(1)

    return data

def scrape_gamelog(url, metadata_map):
    soup = fetch_soup(url)
    table = soup.find("table", {"id": "stats"})
    if not table:
        print(f"    No stats table found at {url}")
        return None

    # --- Extract headers ---
    over_headers = []
    for tr in table.find("thead").find_all("tr")[:-1]:
        cells = tr.find_all("th")
        for cell in cells:
            col_span = int(cell.get("colspan", 1))
            label = cell.get_text(strip=True)
            over_headers.extend([label] * col_span)

    bottom = table.find("thead").find_all("tr")[-1].find_all("th")
    col_pairs = []
    for idx, th in enumerate(bottom):
        if th.get("data-stat") == "ranker":
            continue
        col = th.get_text(strip=True)
        cat = over_headers[idx] if idx < len(over_headers) else ""
        tip = th.get("data-tip", "").strip()
        col_pairs.append((cat, col, tip))

    # --- Map to canonical headers using metadata_map ---
    headers = []
    for cat, col, tip in col_pairs:
        key = (cat, col)
        if key in metadata_map:
            headers.append(metadata_map[key]["unique"])
        else:
            # Unknown header → create fallback, log it
            fallback = f"{cat}_{col}".strip("_")
            headers.append(fallback)
            with open(METADATA_FILE, "a", encoding="utf-8") as f:
                f.write(f"{cat},{col}:{fallback} | {tip}\n")
            print(f"[WARN] New header found: {key}")

    # --- Extract rows ---
    games = []
    for row in table.tbody.find_all("tr"):
        if "class" in row.attrs and "thead" in row["class"]:
            continue
        cells = row.find_all(["th", "td"])
        if not cells or not cells[0].text.strip().isdigit():
            continue

        game_data = {}
        for i, cell in enumerate(cells[1:], start=0):
            if i >= len(headers):
                continue
            header = headers[i]
            val = cell.get_text(strip=True)
            try:
                game_data[header] = float(val) if "." in val else int(val)
            except ValueError:
                game_data[header] = val

        # Fill in missing attributes with 0
        for info in metadata_map.values():
            uniq = info["unique"]
            if uniq not in game_data:
                game_data[uniq] = 0

        week_val = game_data.get("Week")
        if not week_val or not str(week_val).isdigit():
            continue

        game = {
            "week": int(week_val),
            "date": game_data.get("Date", ""),
            "team": game_data.get("Tm") or game_data.get("Team") or "",
            "opponent": game_data.get("Opp", ""),
            "home": game_data.get("game_location", "") != "@",
            "stats": game_data,
        }
        game["fantasy"] = calc_fantasy(game_data)
        games.append(game)

    return {"games": games}


def calc_fantasy(stats):
    """Calculate fantasy points from raw stats dictionary."""
    def g(key):
        val = stats.get(key, 0)
        return val if isinstance(val, (int, float)) else 0

    pass_yds = g("Pass Yds")
    pass_td  = g("Pass TD")
    interceptions = g("Int")

    rush_yds = g("Rush Yds")
    rush_td  = g("Rush TD")

    rec = g("Rec")
    rec_yds = g("Rec Yds")
    rec_td  = g("Rec TD")

    fumbles_lost = g("Fumbles Lost")
    two_pt = g("2PM")

    std = (
        (pass_yds/25) +( pass_td*4) - (interceptions*2)
        + (rush_yds/10) + (rush_td*6)
        + (rec_yds/10) + (rec_td*6)
        + (two_pt*2) - (fumbles_lost*2)
    )
    half = std + rec*0.5
    ppr = std + rec*1.0

    return {"standard": round(std, 2), "half_ppr": round(half, 2), "ppr": round(ppr, 2)}


if __name__ == "__main__":
    # Step 1: Build metadata once
    print("=== Building universal metadata ===")
    metadata_map = get_table_metadata_for_positions(START_YEAR)

    # Step 2: Scrape all players
    all_players = {}
    for year in range(START_YEAR, END_YEAR+1):
        print(f"=== Scraping {year} fantasy players ===")
        for pid, name, url in get_players_for_year(year):
            if pid in seen_players:
                continue
            print(f"Scraping {name} [{pid}]")
            pdata = scrape_player(pid, name, url, metadata_map)  # no getMetadata flag needed now
            if pdata:
                all_players[pid] = pdata
                with open(f"data/{pid}.json", "w") as f:
                    json.dump(pdata, f, indent=2)

