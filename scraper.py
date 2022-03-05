from bs4 import BeautifulSoup, Comment
import requests
import csv
from time import perf_counter
import pandas as pd
import re
from db_helper import Player_Database

db = Player_Database("fantasy_football.db")


# Get all players and url names
years = [year for year in range(2018, 2021)]

# years = [2020]
MAX_RETRY = 3
TEAMS = [
    "ARI",
    "ATL",
    "BAL",
    "BUF",
    "CAR",
    "CHI",
    "CIN",
    "CLE",
    "DAL",
    "DEN",
    "DET",
    "GNB",
    "HOU",
    "IND",
    "JAX",
    "KAN",
    "LAC",
    "LAR",
    "LVR",
    "MIA",
    "MIN",
    "NOR",
    "NWE",
    "NYG",
    "NYJ",
    "OAK",
    "PHI",
    "PIT",
    "SEA",
    "SFO",
    "TAM",
    "TEN",
    "WAS",
]
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
    pattern = "\sJr$|\sJr.$|\sSr$|\sSr.$|\sV$|\sIX$\sIIII$|\sIII$|\sII$|\.|-|'"
    player_name = re.sub(pattern, "", player_name)
    player_name = search_name_format(player_name)
    return player_name.strip()


def clean_year_text(year):
    trash_characters = ["*", "+", "-", "!", "@", "#", "$", "%"]
    for char in trash_characters:
        year = year.replace(char, "")
    return year


def search_name_format(player_name):
    player_name = player_name.replace(" ", "")
    player_name = player_name.lower()
    return player_name


def get_soup(request_object):
    page_content = request_object.content
    soup = BeautifulSoup(page_content, "html.parser")
    return soup


def get_table_rows(table_soup):
    body = table_soup.find("tbody")
    rows = body.find_all("tr")
    return rows


def get_hidden_table(soup, query):
    pass


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
        "2TM": "NFL",
        "Buffalo Bills": "BUF",
        "Miami Dolphins": "MIA",
        "New England Patriots": "NE",
        "New York Jets": "NJY",
        "New York Giants": "NYG",
        "Pittsburgh Steelers": "PIT",
        "Baltimore Ravens": "BAL",
        "Cleveland Browns": "CLE",
        "Cincinnati Bengals": "CIN",
        "Tennessee Titans": "TEN",
        "Indianapolis Colts": "IND",
        "Houston Texans": "HOU",
        "Jacksonville Jaguars": "JAX",
        "Kansas City Chiefs": "KC",
        "Las Vegas Raiders": "LV",
        "Los Angeles Chargers": "LAC",
        "Denver Broncos": "DEN",
        "Chicago Bears": "CHI",
        "Seattle Seahawks": "SEA",
        "San Francisco 49ers": "SF",
        "Arizona Cardinals": "ARI",
        "Green Bay Packers": "GB",
        "Tampa Bay Buccaneers": "TB",
        "Philadelphia Eagles": "PHI",
        "Minnesota Vikings": "MIN",
        "Washington Football Team": "WAS",
        "Carolina Panthers": "CAR",
        "New Orleans Saints": "NO",
        "Los Angeles Rams": "LAR",
        "Detroit Lions": "DET",
        "Atlanta Falcons": "ATL",
        "Washington Redskins": "WAS",
        "Washington Commanders": "WAS"
    }
    return team_mapper[team]


def get_player_yearly_stats(page_url, player_name):
    # print(page_url)
    TRY_COUNT = 0
    while TRY_COUNT < MAX_RETRY:
        try:
            yearly_stats_page = requests.get(page_url)
        except requests.exceptions.ChunkedEncodingError:
            TRY_COUNT += 1
        except requests.exceptions as e:
            TRY_COUNT += 1
        else:
            TRY_COUNT = 0
            break
    else:
        print("Cannot get yearly page for player")
        return
    yearly_stats_soup = get_soup(yearly_stats_page)

    # Parse rushing and receiving table
    comments = yearly_stats_soup.find_all(text=lambda text: isinstance(text, Comment))
    rush_rec_table = 0
    pass_air_yards_table = 0
    pass_accuracy_table = 0
    pass_pressure_table = 0
    for comment in comments:
        comment_soup = BeautifulSoup(comment, "html.parser")
        if comment_soup.find("table", id="detailed_rushing_and_receiving") != None:
            rush_rec_table = comment_soup.find(
                "table", id="detailed_rushing_and_receiving"
            )
        if comment_soup.find("table", id="detailed_receiving_and_rushingg") != None:
            rush_rec_table = comment_soup.find(
                "table", id="detailed_receiving_and_rushing"
            )
        if comment_soup.find("table", id="advanced_air_yards") != None:
            pass_air_yards_table = comment_soup.find("table", id="advanced_air_yards")
        if comment_soup.find("table", "advanced_accuracy") != None:
            pass_accuracy_table = comment_soup.find("table", "advanced_accuracy")
        if comment_soup.find("table", "advanced_pressure") != None:
            pass_pressure_table = comment_soup.find("table", "advanced_pressure")
    if not rush_rec_table:
        rush_rec_table = yearly_stats_soup.find(
            "table", id="detailed_rushing_and_receiving"
        )
        if not rush_rec_table:
            rush_rec_table = yearly_stats_soup.find(
                "table", id="detailed_receiving_and_rushing"
            )
    if not pass_air_yards_table:
        pass_air_yards_table = yearly_stats_soup.find("table", id="advanced_air_yards")
    if not pass_accuracy_table:
        pass_accuracy_table = yearly_stats_soup.find("table", id="advanced_accuracy")
    if not pass_pressure_table:
        pass_pressure_table = yearly_stats_soup.find("table", id="advanced_pressure")
    # print(rush_rec_table)
    if rush_rec_table:
        rush_rec_rows = get_table_rows(rush_rec_table)
        # print("Rush Rows")
        for row in rush_rec_rows:
            rush_stats = {
                "rush_attempts": 0,
                "rush_yards": 0,
                "rush_first_down": 0,
                "rush_yards_before_contact": 0,
                "rush_yards_after_contact": 0,
                "rush_broken_tackles": 0,
                "rush_td": 0,
                "rec_yards": 0,
                "rec_targets": 0,
                "receptions": 0,
                "rec_first_down": 0,
                "rec_air_yards": 0,
                "rec_depth_of_target": 0,
                "rec_broken_tackles": 0,
                "rec_drops": 0,
                "rec_yards_after_catch": 0,
                "rec_int": 0,
                "rec_td": 0,
                "fumbles": 0,
            }
            row_year = clean_year_text(row.find(attrs={"data-stat": "year_id"}).text)
            rush_stats["rush_attempts"] = get_stat(row, "rush_att")
            rush_stats["rush_yards"] = get_stat(row, "rush_yds")
            rush_stats["rush_first_down"] = get_stat(row, "rush_first_down")
            rush_stats["rush_yards_before_contact"] = get_stat(
                row, "rush_yds_before_contact"
            )
            rush_stats["rush_yards_after_contact"] = get_stat(row, "rush_yac")
            rush_stats["rush_broken_tackles"] = get_stat(row, "rush_broken_tackles")
            rush_stats["rush_td"] = get_stat(row, "rush_td")
            rush_stats["rec_targets"] = get_stat(row, "targets")
            rush_stats["receptions"] = get_stat(row, "rec")
            rush_stats["rec_yards"] = get_stat(row, "rec_yds")
            rush_stats["rec_first_down"] = get_stat(row, "rec_first_down")
            rush_stats["rec_air_yards"] = get_stat(row, "rec_air_yds")
            rush_stats["rec_depth_of_target"] = get_stat(row, "rec_adot")
            rush_stats["rec_broken_tackles"] = get_stat(row, "rec_broken_tackles")
            rush_stats["rec_drops"] = get_stat(row, "rec_drops")
            rush_stats["rec_yards_after_catch"] = get_stat(row, "rec_yac")
            rush_stats["rec_int"] = get_stat(row, "rec_target_int")
            rush_stats["rec_td"] = get_stat(row, "rec_td")
            rush_stats["fumbles"] = get_stat(row, "fumbles")
            # print(player_name, row_year, rush_stats)
            db.add_yearly_stats(player_name, row_year, rush_stats)

    # Parse Passing table air yards
    # pass_air_yards_table = yearly_stats_soup.find("table", id="advanced_air_yards")
    if pass_air_yards_table:
        pass_air_rows = get_table_rows(pass_air_yards_table)
        # print("Pass air")
        for row in pass_air_rows:
            pass_air_stats = {
                "pass_yards": 0,
                "pass_attempts": 0,
                "pass_completions": 0,
                "pass_attempt_yards": 0,
                "pass_completed_air_yards": 0,
                "pass_yards_after_catch": 0,
            }
            row_year = clean_year_text(row.find(attrs={"data-stat": "year_id"}).text)
            pass_air_stats["pass_completions"] = get_stat(row, "pass_cmp")
            pass_air_stats["pass_attempts"] = get_stat(row, "pass_att")
            pass_air_stats["pass_yards"] = get_stat(row, "pass_yds")
            pass_air_stats["pass_attempt_yards"] = get_stat(row, "pass_target_yds")
            pass_air_stats["pass_completed_air_yards"] = get_stat(row, "pass_air_yds")
            pass_air_stats["pass_yards_after_catch"] = get_stat(row, "pass_yac_per_cmp")
            # print(pass_air_stats)
            db.add_yearly_stats(player_name, row_year, pass_air_stats)

    # Parse passing table accuracy
    # pass_accuracy_table = yearly_stats_soup.find("table", id="advanced_accuracy")
    if pass_accuracy_table:
        pass_acc_rows = get_table_rows(pass_accuracy_table)
        # print("pass_accuracy")
        for row in pass_acc_rows:
            pass_acc_stats = {
                "passes_batted_down": 0,
                "passes_thrown_away": 0,
                "pass_drops": 0,
                "pass_bad_throws": 0,
                "pass_on_target": 0,
            }
            row_year = clean_year_text(row.find(attrs={"data-stat": "year_id"}).text)
            pass_acc_stats["passes_batted_down"] = get_stat(row, "pass_batted_passes")
            pass_acc_stats["passes_thrown_away"] = get_stat(row, "pass_throwaways")
            pass_acc_stats["pass_drops"] = get_stat(row, "pass_drops")
            pass_acc_stats["pass_bad_throws"] = get_stat(row, "pass_poor_throws")
            pass_acc_stats["pass_on_target"] = get_stat(row, "pass_on_target")
            # print(pass_acc_stats)
            db.add_yearly_stats(player_name, row_year, pass_acc_stats)

    # Parse passing table pressure
    # pass_pressure_table = yearly_stats_soup.find("table", id="advanced_pressure")
    if pass_pressure_table:
        pass_pressure_rows = get_table_rows(pass_pressure_table)
        # print("pass Pressure")
        for row in pass_pressure_rows:
            pass_pressure_stats = {
                "sack": 0,
                "pass_pocket_time": 0,
                "pass_blitzed": 0,
                "pass_hurried": 0,
                "pass_hit": 0,
                "pass_pressured": 0,
                "pass_scramble": 0,
            }
            row_year = clean_year_text(row.find(attrs={"data-stat": "year_id"}).text)
            sack = get_stat(row, "pass_sacked")
            pass_pressure_stats["pass_pocket_time"] = get_stat(row, "pocket_time")
            pass_pressure_stats["pass_blitzed"] = get_stat(row, "pass_blitzed")
            pass_pressure_stats["pass_hurried"] = get_stat(row, "pass_hurried")
            pass_pressure_stats["pass_hit"] = get_stat(row, "pass_hits")
            pass_pressure_stats["pass_pressured"] = get_stat(row, "pass_pressured")
            pass_pressure_stats["pass_scramble"] = get_stat(row, "rush_scrambles")
            # print(pass_pressure_stats)
            db.add_yearly_stats(player_name, row_year, pass_pressure_stats)


def get_player_weekly_stats():
    start_time = perf_counter()
    player_count = 0
    for year in years:
        fantasy_player_list_url = (
            f"https://www.pro-football-reference.com/years/{year}/fantasy.htm"
        )
        TRY_COUNT = 0
        while TRY_COUNT < MAX_RETRY:
            try:
                yearly_stats_page = requests.get(fantasy_player_list_url)
            except requests.exceptions.ChunkedEncodingError:
                TRY_COUNT += 1
            except requests.exceptions as e:
                TRY_COUNT += 1
            else:
                TRY_COUNT = 0
                break
        else:
            print("Cannot get yearly page for player")
            continue
        soup = get_soup(yearly_stats_page)
        player_table = soup.find("table", id="fantasy")
        player_table_rows = get_table_rows(player_table)
        for row in player_table_rows:
            player_count += 1
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
            player_url = f"https://www.pro-football-reference.com/players/{pfr_url_letter}/{pfr_url_name}.htm"

            player_gamelog_url = f"https://www.pro-football-reference.com/players/{pfr_url_letter}/{pfr_url_name}/gamelog"
            TRY_COUNT = 0
            while TRY_COUNT < MAX_RETRY:
                try:
                    stats_page = requests.get(player_gamelog_url)
                except requests.exceptions.ChunkedEncodingError:
                    TRY_COUNT += 1
                else:
                    TRY_COUNT = 0
                    break
            else:
                continue
            stats_soup = get_soup(stats_page)
            birthdate_data = stats_soup.find(id="necro-birth")
            birthdate = birthdate_data["data-birth"]
            db.create_player(player_name, search_player_name, birthdate, pfr_url_name)
            print(year, player_name)
            get_player_yearly_stats(player_url, player_name)
            stats_table = stats_soup.find(id="stats")
            stats_df = pd.read_html(player_gamelog_url)
            stats_rows = get_table_rows(stats_table)
            for row in stats_rows:
                stat_info = {
                    "pass_yards": 0,
                    "pass_attempts": 0,
                    "pass_completions": 0,
                    "pass_int": 0,
                    "pass_td": 0,
                    "sack": 0,
                    "rush_yards": 0,
                    "rush_attempts": 0,
                    "rush_td": 0,
                    "fumbles": 0,
                    "fumbles_lost": 0,
                    "rec_yards": 0,
                    "rec_targets": 0,
                    "rec_td": 0,
                    "receptions": 0,
                    "snap_percentage": 0,
                    "start": 0,
                    "pass_rating": 0,
                    "injured": 0,
                    "two_pt": 0,
                    'kick_returns':0,
                    'kick_return_yards':0,
                    'kick_return_td':0,
                    'punt_returns':0,
                    'punt_return_yards':0,
                    'punt_return_td':0
                }
                row_year = clean_year_text(
                    row.find(attrs={"data-stat": "year_id"}).text
                )
                if row_year == "Year":
                    continue
                did_play = get_stat(row, "reason")
                if not did_play:
                    continue
                # print(row_year)
                if int(row_year) < year:
                    continue
                date = row.find(attrs={"data-stat": "game_date"}).find("a").text
                week = row.find(attrs={"data-stat": "week_num"}).text
                is_home = (
                    0
                    if row.find(attrs={"data-stat": "game_location"}).text == "@"
                    else 1
                )
                team = row.find(attrs={"data-stat": "team"}).find("a").text
                team = team_map(team)
                # print(f"    Week {week} | Home {home_team} | Away {away_team}")
                injured = 1 if "injury" in row["id"] else 0
                stat_info["injured"] = injured
                started = 1 if get_stat(row, "gs") == "*" else 0
                stat_info["start"] = started
                # 1 if row.find(attrs={"data-stat": "gs"}).text == "*" else 0
                stat_info["rush_attempts"] = get_stat(row, "rush_att")
                stat_info["rush_yards"] = get_stat(row, "rush_yds")
                stat_info["rush_td"] = get_stat(row, "rush_td")
                stat_info["rec_targets"] = get_stat(row, "targets")
                stat_info["receptions"] = get_stat(row, "rec")
                stat_info["rec_yards"] = get_stat(row, "rec_yds")
                stat_info["rec_td"] = get_stat(row, "rec_td")
                stat_info["pass_completions"] = get_stat(row, "pass_cmp")
                stat_info["pass_attempts"] = get_stat(row, "pass_att")
                stat_info["pass_yards"] = get_stat(row, "pass_yds")
                stat_info["pass_td"] = get_stat(row, "pass_td")
                stat_info["pass_int"] = get_stat(row, "pass_int")
                stat_info["pass_rating"] = get_stat(row, "pass_rating")
                stat_info["sack"] = get_stat(row, "pass_sacked")
                stat_info["two_pt"] = get_stat(row, "two_pt_md")
                stat_info["fumbles"] = get_stat(row, "fumbles")
                stat_info["fumbles_lost"] = get_stat(row, "fumbles_lost")
                stat_info['kick_returns'] = get_stat(row,'kick_ret')
                stat_info['kick_return_yards'] = get_stat(row,'kick_ret_yds')
                stat_info['kick_return_td'] = get_stat(row,'kick_ret_td')
                stat_info['punt_returns'] = get_stat(row,'punt_ret')
                stat_info['punt_return_yards'] = get_stat(row,'punt_ret_yds')
                stat_info['punt_return_td'] = get_stat(row,'punt_ret_td')
                snap_percent = get_stat(row, "off_pct")
                if type(snap_percent) != int:
                    # Remove % if it exists
                    snap_percent = snap_percent[:-1]
                stat_info["snap_percentage"] = snap_percent
                db.add_weekly_stats(player_name, row_year, week, stat_info)

    stop_time = perf_counter()
    print(
        f"Updated {player_count} players over {len(years)} years in {stop_time - start_time} seconds"
    )


def get_team_rankings():
    for year in years:
        team_data_url = f"https://www.pro-football-reference.com/years/{year}/"
        team_data_page = requests.get(team_data_url)
        soup = get_soup(team_data_page)
        afc_table = soup.find(id="AFC")
        nfc_table = soup.find(id="NFC")
        afc_teams = parse_team_table(afc_table)
        
        nfc_teams = parse_team_table(nfc_table)



def parse_team_table(table):
    rows = get_table_rows(table)
    teams = []
    default_team_info = {
        "team": 0,
        "team_abbr": 0,
        "win_loss_percent": 0,
        "margin_of_victory": 0,
        "strecngth_of_schedule": 0,
        "offense_srs": 0,
        "defense_srs": 0,
    }
    for row in rows:
        # skip every 5 rows due to titles
        if row["data-row"] % 5 == 0:
            continue
        team_info = default_team_info.copy()
        team = row.find(attrs={"data-stat": "team"}).find("a").text
        team_abbr = team_map(team)
        win_loss_percentage = get_stat(row, "win_loss_perc")
        margin_of_victory = get_stat(row, "mov")
        strength_of_schedule = get_stat(row, "sos_total")
        offense_srs = get_stat(row, "srs_offense")
        defense_srs = get_stat(row, "srs_defense")
        team_info["team"] = team
        team_info["team_abbr"] = team_abbr
        team_info["win_loss_percent"] = win_loss_percentage
        team_info["margin_of_victory"] = margin_of_victory
        team_info["strength_of_schedule"] = strength_of_schedule
        team_info["offense_srs"] = offense_srs
        team_info["defense_srs"] = defense_srs
        teams.append(team_info)
    return teams


def get_team_stats(year):
    #year = 2021
    for team in TEAMS:
        page_url = f"https://www.pro-football-reference.com/teams/{team}/{year}.htm"
        TRY_COUNT = 0
        while TRY_COUNT < MAX_RETRY:
            try:
                schedule_url = requests.get(page_url)
            except requests.exceptions.ChunkedEncodingError:
                TRY_COUNT += 1
            except requests.exceptions as e:
                TRY_COUNT += 1
            else:
                TRY_COUNT = 0
                break
        else:
            print(f"Cannot get schedule for {team}")
            continue
        soup = get_soup(schedule_url)
        year_schedule_table = soup.find(id="games")
        schedule_rows = get_table_rows(year_schedule_table)
        for row in schedule_rows:
            week_number = get_stat(row, "week_num")
            opponent = get_stat(row, "opponent")
            if opponent == "Bye Week":
                bye_week = 1

                continue
            else:
                opponent = team_map(opponent)
            date = row.find(attrs={"data-stat": "game_date"})["csk"]
            time = row.find(attrs={'data--stat': "game_time"})['csk']
            game_outcome = row.find(attrs={"data-stat": "game_outcome"}).text
            if game_outcome == "W":
                win = 1
            elif game_outcome == "L":
                win = 0
            else:
                win = -1
            is_home = (
                0 if row.find(attrs={"data-stat": "game_location"}).text == "@" else 1
            )
            team_score = get_stat(row, "pts_off")
            opponent_score = get_stat(row, "pts_def")
            overtime = 1 if get_stat(row,'overtime') == 'OT' else 0
            first_downs = int(get_stat(row,'first_down_off'))
            pass_yards = int(get_stat(row,'pass_yds_off'))
            rush_yards = int(get_stat(row,'rush_yds_off'))
            turnovers = int(get_stat(row,'to_off'))
            first_downs_allowed = int(get_stat(row,'first_down_def'))
            pass_yards_allowed = int(get_stat(row,'pass_yds_def'))
            rush_yards_allowed = int(get_stat(row,'rush_yds_def'))
            turnovers_achieved = int(get_stat(row,'to_def'))
            #TODO Log to DB for this game
            team_year_stats = { 'total_yards':0,
                                'pass_yards':0,
                                'pass_attempts':0,
                                'pass_completions':0,
                                'pass_int':0,
                                'pass_td':0,
                                'sack':0,
                                'sack_yards':0,
                                'rush_yards':0,
                                'rush_attempts':0,
                                'fumbles':0,
                                'fumbles_lost':0,
                                'rec_yards':0,
                                'rec_targets':0,
                                'receptions':0,
                                'rush_first_down':0,
                                'rush_yards_before_contact':0,
                                'rush_yards_after_contact':0,
                                'rush_broken_tackles':0,
                                'rec_first_down':0,
                                'rec_air_yards':0,
                                'rec_depth_of_target':0,
                                'rec_drops':0,
                                'rec_broken_tackles':0,
                                'rec_yards_after_catch':0,
                                'pass_attempt_yards':0,
                                'pass_completed_air_yards':0,
                                'pass_yards_after_catch':0,
                                'passes_batted_down':0,
                                'passes_thrown_away':0,
                                'pass_drops':0,
                                'pass_bad_throws':0,
                                'pass_on_target':0,
                                'pass_pocket_time':0,
                                'pass_blitzed':0,
                                'pass_hurried':0,
                                'pass_hit':0,
                                'pass_pressured':0,
                                'pass_scramble':0,
                                'rush_td':0,
                                'rec_td':0,
                                'points_for':0,
                                'points_against':0,
                                'play_count':0,
                                'first_downs':0,
                                'penalties':0,
                                'penalty_yards':0,
                                'drive_count':0,
                                'avg_drive_time':0}
        #Yearly plays & Penalties
        year_stats_table = soup.find(id='team_stats')
        year_stats_rows = get_table_rows(year_stats_table)
        team_stats = year_stats_rows[0]
        team_year_stats['points_for'] = int(get_stat(team_stats,'points'))
        team_year_stats['total_yards'] = int(get_stat(team_stats,'total_yards'))
        team_year_stats['play_count'] = int(get_stat(team_stats,'playes_offense'))
        team_year_stats['first_downs'] = int(get_stat(team_stats,'first_down'))
        team_year_stats['rec_first_down'] = int(get_stat(team_stats,'pass_fd'))
        team_year_stats['rush_first_down'] = int(get_stat(team_stats,'rush_fd'))
        team_year_stats['penalties'] = int(get_stat(team_stats,'penalties'))
        team_year_stats['penalty_yards'] = int(get_stat(team_stats,'penalties_yds'))
        team_year_stats['drive_count'] = int(get_stat(team_stats,'drives'))
        team_year_stats['avg_drive_time'] = get_stat(team_stats,'time_avg')
        #TODO Log to DB     

        #Yearly Conversion Stats
        conversion_table = soup.find(id='team_conversions')
        conversion_rows = get_table_rows(conversion_table)
        conversion_row = conversion_rows[0]
        third_down_attempts = int(get_stat(conversion_row,'third_down_att'))
        third_down_success = int(get_stat(conversion_row,'third_down_success'))
        fourth_down_attempts = int(get_stat(conversion_row,'fourth_down_att'))
        fourth_down_success = int(get_stat(conversion_row,'fourth_down_success'))
        red_zone_attempts = int(get_stat(conversion_row,'red_zone_att'))
        red_zone_success = int(get_stat(conversion_row,'red_zone_scores'))
        #TODO Log to DB    


        #Yearly Passing Stats
        passing_table = soup.find(id='passing')
        team_stats = passing_table.find('tfoot')
        team_stats = team_stats.find_all('tr')[0]
        pass_yardscompletions = int(get_stat(team_stats,'pass_cmp'))
        attempts = int(get_stat(team_stats,'pass_att'))
        pass_td = int(get_stat(team_stats,'pass_td'))
        pass_int = int(get_stat(team_stats,'pass_int'))
        sack_count = int(get_stat(team_stats,'pass_sacked'))
        sack_yards = int(get_stat(team_stats,'pass_sacked_yds'))
        #TODO Log to DB    
        




def get_fantasy_stats(week):
    positions = ["qb", "wr", "te", "rb"]
    for pos in positions:
        url = f"https://www.fantasypros.com/nfl/projections/{pos}.php?scoring=PPR&week={week}"
        data = requests.get(url)
        data_content = data.content
        soup = BeautifulSoup(data_content, "html.parser")
        fantasy_table = soup.find(id="data")
        fantasy_table = get_table_rows(fantasy_table)
        for player_row in fantasy_table:
            player_name = player_row.find(attrs={"class": "player-name"}).text
            player_name = name_format(player_name)
            fantasy_points = player_row.find_all("td")[-1]["data-sort-value"]
            all_player_fantasy_points[player_name] = fantasy_points


if __name__ == "__main__":
    # parse_yearly_data(
    #     "https://www.pro-football-reference.com/players/H/HenrDe00.htm", "Derrick Henry"
    # )