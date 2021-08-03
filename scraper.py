from bs4 import BeautifulSoup
import requests
import csv


# Get all players and url names
years = [year for year in range(2019, 2021)]
# years = [2020]
MAX_RETRY = 3


def get_stat(row, stat_name):
    if row.find(attrs={"data-stat": stat_name}) != None:
        if row.find(attrs={"data-stat": stat_name}).text == "":
            return 0
        else:
            return row.find(attrs={"data-stat": stat_name}).text
    else:
        return 0


with open("data.csv", "w") as file:
    writer = csv.writer(file)
    header = [
        "player_name",
        "pos",
        "year",
        "date",
        "week",
        "is_away",
        "home_team",
        "away_team",
        "injured",
        "started",
        "rush_attempts",
        "rush_yards",
        "rush_td",
        "rec_receptions",
        "rec_targets",
        "rec_td",
        "pass_completions",
        "pass_attempts",
        "pass_int",
        "pass_rating",
        "sacks",
        "sack_yards",
        "two_pt",
        "fumbles",
        "fumbles_lost",
        "snap_percent",
    ]
    writer.writerow(header)

    for year in years:
        fantasy_player_list_url = (
            f"https://www.pro-football-reference.com/years/{year}/fantasy.htm"
        )
        fantasy_player_page = requests.get(fantasy_player_list_url)
        fantasy_player_page_content = fantasy_player_page.content
        soup = BeautifulSoup(fantasy_player_page_content, "html.parser")
        player_table = soup.find(id="fantasy")
        player_table_body = player_table.find("tbody")
        player_table_rows = player_table_body.find_all("tr")
        for row in player_table_rows:
            player_name_data = row.find(attrs={"data-stat": "player"})
            if player_name_data.text == "Player":
                # print("row break")
                continue
            player_name = player_name_data.find("a").text
            pos = row.find(attrs={"data-stat": "fantasy_pos"}).text
            pfr_url_name = player_name_data["data-append-csv"]
            pfr_url_letter = pfr_url_name[0]
            player_gamelog_url = f"https://www.pro-football-reference.com/players/{pfr_url_letter}/{pfr_url_name}/gamelog"
            TRY_COUNT = 0
            while TRY_COUNT < 3:
                try:
                    stats_page = requests.get(player_gamelog_url)
                except requests.exceptions.ChunkedEncodingError:
                    TRY_COUNT += 1
                else:
                    TRY_COUNT = 0
                    break
            else:
                writer.writerow([player_name])
                continue
            stats_page_content = stats_page.content
            stats_soup = BeautifulSoup(stats_page_content, "html.parser")
            birthdate_data = stats_soup.find(id="necro-birth")
            birthdate = birthdate_data["data-birth"]
            print(year, player_name)
            stats_table = stats_soup.find(id="stats").find("tbody")
            stats_rows = stats_table.find_all("tr")
            for row in stats_rows:
                row_year = row.find(attrs={"data-stat": "year_id"}).text
                if row_year == "Year":
                    continue
                # print(row_year)
                if int(row_year) < year:
                    continue
                date = row.find(attrs={"data-stat": "game_date"}).find("a").text
                week = row.find(attrs={"data-stat": "week_num"}).text
                is_away = (
                    1
                    if row.find(attrs={"data-stat": "game_location"}).text == "@"
                    else 0
                )
                home_team = row.find(attrs={"data-stat": "team"}).find("a").text
                away_team = row.find(attrs={"data-stat": "opp"}).find("a").text
                if is_away:
                    home_team, away_team = away_team, home_team
                # print(f"    Week {week} | Home {home_team} | Away {away_team}")
                if "injury" in row["id"]:
                    injured = 1
                    pass
                else:
                    injured = 0
                    started = (
                        1 if row.find(attrs={"data-stat": "gs"}).text == "*" else 0
                    )
                    rush_attempts = get_stat(row, "rush_att")
                    rush_yards = get_stat(row, "rush_yds")
                    rush_td = get_stat(row, "rush_td")
                    rec_targets = get_stat(row, "targets")
                    rec_receptions = get_stat(row, "rec")
                    rec_yards = get_stat(row, "rec_yds")
                    rec_td = get_stat(row, "rec_td")
                    pass_completions = get_stat(row, "pass_cmp")
                    pass_attempts = get_stat(row, "pass_att")
                    pass_yards = get_stat(row, "pass_yds")
                    pass_td = get_stat(row, "pass_td")
                    pass_int = get_stat(row, "pass_int")
                    pass_rating = get_stat(row, "pass_rating")
                    sacks = get_stat(row, "pass_sacked")
                    sack_yards = get_stat(row, "pass_sacked_yds")
                    two_pt = get_stat(row, "two_pt_md")
                    fumbles = get_stat(row, "fumbles")
                    fumbles_lost = get_stat(row, "fumbles_lost")
                    snap_percent = get_stat(row, "off_pct")
                    snap_percent = snap_percent[:-1]

                    csv_row = [
                        player_name,
                        pos,
                        row_year,
                        date,
                        week,
                        is_away,
                        home_team,
                        away_team,
                        injured,
                        started,
                        rush_attempts,
                        rush_yards,
                        rush_td,
                        rec_receptions,
                        rec_targets,
                        rec_td,
                        pass_completions,
                        pass_attempts,
                        pass_int,
                        pass_rating,
                        sacks,
                        sack_yards,
                        two_pt,
                        fumbles,
                        fumbles_lost,
                        snap_percent,
                    ]
                    writer.writerow(csv_row)
