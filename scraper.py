from bs4 import BeautifulSoup
import requests
import csv
from time import perf_counter
import pandas as pd


# Get all players and url names
years = [year for year in range(2019, 2021)]
# years = [2020]
MAX_RETRY = 3
all_player_fantasy_points = {}


def get_stat(row, stat_name):
    if row.find(attrs={"data-stat": stat_name}) != None:
        if row.find(attrs={"data-stat": stat_name}).text == "":
            return 0
        else:
            return row.find(attrs={"data-stat": stat_name}).text
    else:
        return 0


def name_format(player_name):
    trash_characters = [".", "-", "'", "Jr", "IIII", "III", "II"]
    for char in trash_characters:
        player_name = player_name.replace(char, "")
    return player_name.strip()


def search_name_format(player_name):
    player_name = player_name.replace(" ", "")
    player_name = player_name.lower()
    return player_name


def team_map(team):
    team_mapper = {
        "ARI": "ARI",
        "ATL": "ATL",
        "BAL": "BAL",
        "BUF": "BUF",
        "CAR": "CAR",
        "CHI": "CHI",
        "CIN": "CIN",
        "CLE": "CLE",
        "DAL": "DAL",
        "DEN": "DEN",
        "DET": "DET",
        "GNB": "GB",
        "HOU": "HOU",
        "IND": "IND",
        "JAX": "JAX",
        "KAN": "KC",
        "LAC": "LAC",
        "LAR": "LAR",
        "LVR": "LV",
        "MIA": "MIA",
        "MIN": "MIN",
        "NOR": "NO",
        "NWE": "NE",
        "NYG": "NYG",
        "NYJ": "NYJ",
        "OAK": "OAK",
        "PHI": "PHI",
        "PIT": "PIT",
        "SEA": "SEA",
        "SFO": "SF",
        "TAM": "TB",
        "TEN": "TEN",
        "WAS": "WAS",
        'Buffalo Bills': 'BUF',
        'Miami Dolphins': 'MIA',
        'New England Patriots': 'NE',
        'New York Jets': 'NJY',
        'New York Giants': 'NYG',
        'Pittsburgh Steelers': 'PIT',
        'Baltimore Ravens': 'BAL',
        'Cleveland Browns': 'CLE',
        'Cincinnati Bengals': 'CIN',
        'Tennessee Titans': 'TEN',
        'Indianapolis Colts': 'IND',
        'Houston Texans': 'HOU',
        'Jacksonville Jaguars': 'JAX',
        'Kansas City Chiefs': 'KC',
        'Las Vegas Raiders': 'LV',
        'Los Angeles Chargers': 'LAC',
        'Denver Broncos': 'DEN'
        }
    return team_mapper[team]

def get_player_stats():
    start_time = perf_counter()
    with open("data.csv", "w") as file:
        writer = csv.writer(file)
        header = [
            "player_name",
            "search_player_name",
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
                player_name = name_format(player_name)
                search_player_name = search_name_format(player_name)
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
                stats_df = pd.read_html(player_gamelog_url)
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
                    home_team = team_map(home_team)
                    away_team = team_map(away_team)
                    # print(f"    Week {week} | Home {home_team} | Away {away_team}")
                    injured = 1 if "injury" in row["id"] else 0
                    started = 1 if get_stat(row, "gs") == "*" else 0
                    # 1 if row.find(attrs={"data-stat": "gs"}).text == "*" else 0
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
                    if type(snap_percent) != int:
                        # Remove % if it exists
                        snap_percent = snap_percent[:-1]

                    csv_row = [
                        player_name,
                        search_player_name,
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
    stop_time = perf_counter()
    print(stop_time - start_time)
`
def get_team_rankings():
    for year in years:
        team_data_url = f'https://www.pro-football-reference.com/years/{year}/'
        team_data_page = requests.get(team_data_url)
        teaam_page_content = team_data_page.content
        soup = BeautifulSoup(teaam_page_content,'html.parser')
        afc_table = soup.find(id="AFC")
        nfc_table = soup.find(id='NFC')
        afc_teams = parse_team_table(afc_table)
        nfc_teams = parse_team_table(nfc_table)

def parse_team_table(table):
    table_body = table.find('tbody')
    rows = table_body.find_all('tr')
    teams = []
    default_team_info = {'team': 0,'team_abbr':0,'win_loss_percent':0,'margin_of_victory':0,'strecngth_of_schedule':0,'offense_srs':0,'defense_srs':0}
    for row in rows:
        #skip every 5 rows due to titles
        if row['data-row']%5 == 0:
            continue
        team_info = default_team_info.copy()
        team = row.find(attrs={"data-stat": "team"}).find("a").text
        team_abbr = team_map(team)
        win_loss_percentage = get_stat(row,'win_loss_perc')
        margin_of_victory = get_stat(row,'mov')
        strength_of_schedule = get_stat(row,'sos_total')
        offense_srs = get_stat(row,'srs_offense')
        defense_srs = get_stat(row,'srs_defense')
        team_info['team'] = team
        team_info['team_abbr'] = team_abbr
        team_info['win_loss_percent'] = win_loss_percentage
        team_info['margin_of_victory'] = margin_of_victory
        team_info['strength_of_schedule'] = strength_of_schedule
        team_info['offense_srs'] = offense_srs
        team_info['defense_srs'] = defense_srs
        teams.append(team_info)
    return teams

def get_fantasy_stats(week):
    positions = ['qb','wr','te','rb']
    for pos in positions:
        url = f'https://www.fantasypros.com/nfl/projections/{pos}.php?scoring=PPR&week={week}'
        data = requests.get(url)
        data_content = data.content
        soup = BeautifulSoup(data_content,'html.parser')
        fantasy_table = soup.find(id='data')
        fantasy_table = fantasy_table.find('tbody')
        for player_row in fantasy_table:
            player_name = player_row.find(class='player-name').text
            player_name = name_format(player_name)
            fantasy_points = player_row.find_all('td')[-1]['data-sort-value']
            all_player_fantasy_points[player_name] = fantasy_points