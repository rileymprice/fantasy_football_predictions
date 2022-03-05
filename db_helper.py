from log_helper import Logger
import sqlite3

logger = Logger(__name__)


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


class Player_Database:
    def __init__(self, db):
        self.db = db
        self.conn = sqlite3.connect(db)
        self.conn.row_factory = dict_factory
        self.cur = self.conn.cursor()
        self.cur.execute(
            """CREATE TABLE IF NOT EXISTS players (
                                                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                                                    full_name    VARCHAR,
                                                    birth_date   DATE,
                                                    espn_id      INT,
                                                    pfr_url_name VARCHAR,
                                                    search_name  VARCHAR,
                                                    weight       INTEGER DEFAULT (0),
                                                    height       INTEGER DEFAULT (0) 
                                                );"""
        )
        self.cur.execute(
            """CREATE TABLE IF NOT EXISTS teams (
                                                    id        INTEGER PRIMARY KEY AUTOINCREMENT,
                                                    name      VARCHAR,
                                                    name_abbr VARCHAR
                                                );"""
        )
        self.cur.execute(
            """CREATE TABLE IF NOT EXISTSschedule (
                                        id             INTEGER PRIMARY KEY AUTOINCREMENT,
                                        year           INT     NOT NULL,
                                        week           INT     NOT NULL,
                                        date           DATE,
                                        time           TIME,
                                        team           VARCHAR REFERENCES teams (id),
                                        opponent       VARCHAR REFERENCES teams (id),
                                        team_score     INT,
                                        opponent_score INT,
                                        bye_week       BOOLEAN,
                                        is_home        BOOLEAN
                                    );"""
        )
        self.cur.execute(
            """CREATE TABLE IS NOT EXISTS rosters (
                                    id        INTEGER PRIMARY KEY AUTOINCREMENT,
                                    year      INT,
                                    player_id INT     REFERENCES players (id),
                                    team_id   INT     REFERENCES teams (id) 
                                );"""
        )
        self.cur.execute(
            """CREATE TABLE IF NOT EXISTS weekly_rankings (
                                            team_id              INT    PRIMARY KEY
                                                                        REFERENCES teams (team_id),
                                            week                 INT,
                                            offense_rank         INT,
                                            defense_rank         INT,
                                            year                 INT,
                                            margin_of_victory    DOUBLE,
                                            strength_of_schedule DOUBLE,
                                            win_loss_percent     DOUBLE,
                                            offense_srs          DOUBLE,
                                            defense_srs          DOUBLE
                                        );"""
        )
        self.cur.execute(
            """CREATE TABLE IF NOT EXISTS player_weekly_stats (
                                                weekly_stat_id    INTEGER PRIMARY KEY AUTOINCREMENT,
                                                player_id         INT     REFERENCES players (id) 
                                                                        NOT NULL,
                                                team_id           INT     REFERENCES teams (id),
                                                week              INT     DEFAULT (0),
                                                pass_yards        INT     DEFAULT (0),
                                                pass_attempts     INT     DEFAULT (0),
                                                pass_completions  INT     DEFAULT (0),
                                                pass_int          INT     DEFAULT (0),
                                                pass_td           INT     DEFAULT (0),
                                                sack              INT     DEFAULT (0),
                                                rush_yards        INT     DEFAULT (0),
                                                rush_attempts     INT     DEFAULT (0),
                                                fumbles           INT     DEFAULT (0),
                                                fumbles_lost      INT     DEFAULT (0),
                                                rec_yards         INT     DEFAULT (0),
                                                rec_targets       INT     DEFAULT (0),
                                                receptions        INT     DEFAULT (0),
                                                year              INT     NOT NULL
                                                                        DEFAULT (0),
                                                snap_percentage   INT     DEFAULT (0),
                                                fantasy_points    DOUBLE  DEFAULT (0),
                                                start             BOOLEAN DEFAULT (0),
                                                pass_rating       DOUBLE  DEFAULT (0),
                                                injured           BOOLEAN DEFAULT (0),
                                                rush_td           INT     DEFAULT (0),
                                                rec_td            INT     DEFAULT (0),
                                                two_pt            INT     DEFAULT (0),
                                                kick_returns      INT     DEFAULT (0),
                                                kick_return_yards INT     DEFAULT (0),
                                                kick_return_td    INT     DEFAULT (0),
                                                punt_returns      INT     DEFAULT (0),
                                                punt_return_yards INT     DEFAULT (0),
                                                punt_return_td    INT     DEFAULT (0) 
                                            );"""
        )

        self.cur.execute(
            """CREATE TABLE IF NOT EXISTS player_yearly_stats (
                                                    yearly_stats_id           INTEGER PRIMARY KEY AUTOINCREMENT,
                                                    player_id                 INT     REFERENCES players (id),
                                                    pass_yards                INT     DEFAULT (0),
                                                    pass_attempts             INT     DEFAULT (0),
                                                    pass_completions          INT     DEFAULT (0),
                                                    pass_int                  INT     DEFAULT (0),
                                                    pass_td                   INT     DEFAULT (0),
                                                    sack                      INT     DEFAULT (0),
                                                    rush_yards                INT     DEFAULT (0),
                                                    rush_attempts             INT     DEFAULT (0),
                                                    fumbles                   INT     DEFAULT (0),
                                                    fumbles_lost              INT     DEFAULT (0),
                                                    rec_yards                 INT     DEFAULT (0),
                                                    rec_targets               INT     DEFAULT (0),
                                                    receptions                INT     DEFAULT (0),
                                                    year                      INT     NOT NULL,
                                                    snap_percentage           INT     DEFAULT (0),
                                                    fantasy_points            DOUBLE  DEFAULT (0),
                                                    start                     BOOLEAN DEFAULT (0),
                                                    pass_rating               DOUBLE  DEFAULT (0),
                                                    injured                   BOOLEAN DEFAULT (0),
                                                    rush_first_down           INT     DEFAULT (0),
                                                    rush_yards_before_contact INT     DEFAULT (0),
                                                    rush_yards_after_contact  INT     DEFAULT (0),
                                                    rush_broken_tackles       INT     DEFAULT (0),
                                                    rec_first_down            INT     DEFAULT (0),
                                                    rec_air_yards             INT     DEFAULT (0),
                                                    rec_depth_of_target       DOUBLE  DEFAULT (0),
                                                    rec_drops                 INT     DEFAULT (0),
                                                    rec_broken_tackles        INT     DEFAULT (0),
                                                    rec_yards_after_catch     INT     DEFAULT (0),
                                                    pass_attempt_yards        INT     DEFAULT (0),
                                                    pass_completed_air_yards  INT     DEFAULT (0),
                                                    pass_yards_after_catch    INT     DEFAULT (0),
                                                    passes_batted_down        INT     DEFAULT (0),
                                                    passes_thrown_away        INT     DEFAULT (0),
                                                    pass_drops                INT     DEFAULT (0),
                                                    pass_bad_throws           INT     DEFAULT (0),
                                                    pass_on_target            INT     DEFAULT (0),
                                                    pass_pocket_time          DOUBLE  DEFAULT (0),
                                                    pass_blitzed              INT     DEFAULT (0),
                                                    pass_hurried              INT     DEFAULT (0),
                                                    pass_hit                  INT     DEFAULT (0),
                                                    pass_pressured            INT     DEFAULT (0),
                                                    pass_scramble             INT     DEFAULT (0),
                                                    rush_td                   INT     DEFAULT (0),
                                                    rec_td                    INT     DEFAULT (0),
                                                    rec_int                   INT     DEFAULT (0) 
                                                );
                                    """
        )

        self.cur.execute(
            """CREATE TABLE IF NOT EXISTS team_weekly_stats (
                                                                            yearly_stats_id           INTEGER PRIMARY KEY AUTOINCREMENT,
                                                                            team_id                   INT     REFERENCES teams (id),
                                                                            year                      INT     NOT NULL,
                                                                            week                      INT     NOT NULL,
                                                                            pass_yards                INT     DEFAULT (0),
                                                                            pass_attempts             INT     DEFAULT (0),
                                                                            pass_completions          INT     DEFAULT (0),
                                                                            pass_int                  INT     DEFAULT (0),
                                                                            pass_td                   INT     DEFAULT (0),
                                                                            sack                      INT     DEFAULT (0),
                                                                            rush_yards                INT     DEFAULT (0),
                                                                            rush_attempts             INT     DEFAULT (0),
                                                                            fumbles                   INT     DEFAULT (0),
                                                                            fumbles_lost              INT     DEFAULT (0),
                                                                            rec_yards                 INT     DEFAULT (0),
                                                                            rec_targets               INT     DEFAULT (0),
                                                                            receptions                INT     DEFAULT (0),
                                                                            pass_rating               DOUBLE  DEFAULT (0),
                                                                            rush_first_down           INT     DEFAULT (0),
                                                                            rush_yards_before_contact INT     DEFAULT (0),
                                                                            rush_yards_after_contact  INT     DEFAULT (0),
                                                                            rush_broken_tackles       INT     DEFAULT (0),
                                                                            rec_first_down            INT     DEFAULT (0),
                                                                            rec_air_yards             INT     DEFAULT (0),
                                                                            rec_depth_of_target       DOUBLE  DEFAULT (0),
                                                                            rec_drops                 INT     DEFAULT (0),
                                                                            rec_broken_tackles        INT     DEFAULT (0),
                                                                            rec_yards_after_catch     INT     DEFAULT (0),
                                                                            pass_attempt_yards        INT     DEFAULT (0),
                                                                            pass_completed_air_yards  INT     DEFAULT (0),
                                                                            pass_yards_after_catch    INT     DEFAULT (0),
                                                                            passes_batted_down        INT     DEFAULT (0),
                                                                            passes_thrown_away        INT     DEFAULT (0),
                                                                            pass_drops                INT     DEFAULT (0),
                                                                            pass_bad_throws           INT     DEFAULT (0),
                                                                            pass_on_target            INT     DEFAULT (0),
                                                                            pass_pocket_time          DOUBLE  DEFAULT (0),
                                                                            pass_blitzed              INT     DEFAULT (0),
                                                                            pass_hurried              INT     DEFAULT (0),
                                                                            pass_hit                  INT     DEFAULT (0),
                                                                            pass_pressured            INT     DEFAULT (0),
                                                                            pass_scramble             INT     DEFAULT (0),
                                                                            rush_td                   INT     DEFAULT (0),
                                                                            rec_td                    INT     DEFAULT (0) 
                                                                        );
"""
        )
        self.cur.execute(
            """CREATE TABLE IF NOT EXISTS team_yearly_stats (
                                                            yearly_stats_id           INTEGER PRIMARY KEY AUTOINCREMENT,
                                                            team_id                   INT     REFERENCES teams (id),
                                                            pass_yards                INT     DEFAULT (0),
                                                            pass_attempts             INT     DEFAULT (0),
                                                            pass_completions          INT     DEFAULT (0),
                                                            pass_int                  INT     DEFAULT (0),
                                                            pass_td                   INT     DEFAULT (0),
                                                            sack                      INT     DEFAULT (0),
                                                            rush_yards                INT     DEFAULT (0),
                                                            rush_attempts             INT     DEFAULT (0),
                                                            fumbles                   INT     DEFAULT (0),
                                                            fumbles_lost              INT     DEFAULT (0),
                                                            rec_yards                 INT     DEFAULT (0),
                                                            rec_targets               INT     DEFAULT (0),
                                                            receptions                INT     DEFAULT (0),
                                                            year                      INT     NOT NULL,
                                                            pass_rating               DOUBLE  DEFAULT (0),
                                                            rush_first_down           INT     DEFAULT (0),
                                                            rush_yards_before_contact INT     DEFAULT (0),
                                                            rush_yards_after_contact  INT     DEFAULT (0),
                                                            rush_broken_tackles       INT     DEFAULT (0),
                                                            rec_first_down            INT     DEFAULT (0),
                                                            rec_air_yards             INT     DEFAULT (0),
                                                            rec_depth_of_target       DOUBLE  DEFAULT (0),
                                                            rec_drops                 INT     DEFAULT (0),
                                                            rec_broken_tackles        INT     DEFAULT (0),
                                                            rec_yards_after_catch     INT     DEFAULT (0),
                                                            pass_attempt_yards        INT     DEFAULT (0),
                                                            pass_completed_air_yards  INT     DEFAULT (0),
                                                            pass_yards_after_catch    INT     DEFAULT (0),
                                                            passes_batted_down        INT     DEFAULT (0),
                                                            passes_thrown_away        INT     DEFAULT (0),
                                                            pass_drops                INT     DEFAULT (0),
                                                            pass_bad_throws           INT     DEFAULT (0),
                                                            pass_on_target            INT     DEFAULT (0),
                                                            pass_pocket_time          DOUBLE  DEFAULT (0),
                                                            pass_blitzed              INT     DEFAULT (0),
                                                            pass_hurried              INT     DEFAULT (0),
                                                            pass_hit                  INT     DEFAULT (0),
                                                            pass_pressured            INT     DEFAULT (0),
                                                            pass_scramble             INT     DEFAULT (0),
                                                            rush_td                   INT     DEFAULT (0),
                                                            rec_td                    INT     DEFAULT (0) 
                                                        );"""
        )

    def create_player(
        self, full_name, search_name, birth_date, pfr_url_name, espn_id=00000
    ):
        player_exist = self.cur.execute(
            "SELECT count(*) FROM players WHERE search_name = ?", (search_name,)
        )
        player_exist = player_exist.fetchone()["count(*)"]
        if player_exist:
            logger.info(f"{full_name} already exists in DB")
        else:
            try:
                logger.info(
                    f"Adding player name={full_name} search_name={search_name} birthdate={birth_date} pfr_url={pfr_url_name} espn_id={espn_id} "
                )
                self.cur.execute(
                    "INSERT INTO players (full_name,search_name,birth_date,pfr_url_name,espn_id) VALUES (?,?,?,?,?)",
                    (
                        full_name,
                        search_name,
                        birth_date,
                        pfr_url_name,
                        espn_id,
                    ),
                )
            except sqlite3.Error as error:
                logger.exception(f"Error adding player {full_name}|| {error}")
            else:
                self.conn.commit()

    def get_player_id(self, player_name):
        logger.info(f"Getting player id for {player_name}")
        try:
            name_data = self.cur.execute(
                "SELECT player_id FROM players WHERE full_name = ?", (player_name,)
            )
        except sqlite3.Error as error:
            logger.exception(f"error getting id for {player_name}")
        else:
            player_id = name_data.fetchone()
            return player_id["player_id"]

    def create_team(self, team_name, team_abbr):
        try:
            logger.info(f"Adding team team_name={team_name} team_abbr={team_abbr}")
            self.cur.execute(
                "INTO INTO teams (team_name,team_abbr) VALUES (?,?)",
                (team_name, team_abbr),
            )
        except sqlite3.Error as error:
            logger.exception(f"Error adding team {team_name}|| {error}")
        else:
            self.conn.commit()
            logger.info(f"Successfully added {team_name}")

    def get_team_id(self, team_name="", team_abbr=""):
        if team_name:
            logger.info(f"Getting team id for {team_name}")
            try:
                name_data = self.cur.execute(
                    "SELECT team_id FROM teams WHERE team_name = ?", (team_name,)
                )
            except sqlite3.Error as error:
                logger.exception(f"Error getting team id for {team_name} || {error}")
            else:
                team_id = name_data.fetchone()
                return team_id["team_id"]
        else:
            logger.info(f"Getting team id for {team_abbr}")
            try:
                name_data = self.cur.execute(
                    "SELECT team_id FROM teams WHERE team_abbr = ?", (team_abbr,)
                )
            except sqlite3.Error as error:
                logger.exception(f"Error getting team id for {team_abbr} || {error}")
            else:
                team_id = name_data.fetchone()
                return team_id

    def add_schedule(
        self,
        team,
        opponent,
        date,
        week,
        year,
        is_home,
        team_score=-1,
        opponent_score=-1,
    ):
        logger.info(
            f"Adding new schedule date={date} week={week} team={team} opponent={opponent}"
        )
        try:
            if team_score == -1:
                self.cur.execute(
                    "INSERT INTO schedule (team,opponent,date,week,year,is_home) VALUES (?,?,?,?,?,?)",
                    (team, opponent, date, week, year, is_home),
                )
            else:
                schedule_exists = self.cur.execute(
                    "SELECT COUNT(*) FROM schedule WHERE year=? AND week=? AND team=? AND opponent = ?"
                ).fetchone()
                if schedule_exists:
                    self.cur.execute(
                        "UPDATE schedule SET team_score = ?,opponent_score = ? WHERE year=? AND team=? AND opponent = ? AND week = ?",
                        (team_score, opponent_score, year, team, opponent, week),
                    )
                else:
                    self.cur.execute(
                        "INSERT INTO schedule (team,opponent,date,week,is_home,team_score,opponent_score) VALUES (?,?,?,?,?,?,?)",
                        (
                            team,
                            opponent,
                            date,
                            week,
                            is_home,
                            team_score,
                            opponent_score,
                        ),
                    )
        except sqlite3.Error as error:
            logger.exception(
                f"Error adding schedule for week {week} team {team} opponent {opponent}"
            )
        else:
            self.conn.commit()
            logger.info(
                f"Successfully created schedule for {week} team={team} opponent={opponent}"
            )

    def add_bye_week(self, team_abbr, week, year):
        logger.info(f"Adding bye week for {team_abbr} week {week} year {year}")
        try:
            self.cur.execute(
                "INSERT INTO schedule (team,year,week) VALUES (?,?,?)",
                (
                    team_abbr,
                    year,
                    week,
                ),
            )
        except sqlite3.Error as error:
            logger.exception(
                f"Error adding bye week for {team_abbr} on week {week} year {year}"
            )
        else:
            self.conn.commit()

    def get_ranked_rankings(self, is_offense, week, year):
        team_type = "offense" if is_offense == True else "defense"
        try:
            logger.info("Getting ranked rankings")
            rankings = self.cur.execute(
                "SELECT team_id, ?_srs FROM weekly_rankings WHERE week = ? AND year = year",
                (team_type, week, year),
            )
        except sqlite3.Error as error:
            logger.exception(
                f"Failed getting ranked rankings for week {week} year {year}"
            )
        else:
            logger.info(f"Retrieved ranked rankings for week {week} on year {year}")
            rankings = rankings.fetchall()
            temp_rankings = []
            for ranking in rankings:
                temp_rankings.append((ranking["team_id"], ranking[f"{team_type}_srs"]))
            temp_rankings.sort(key=lambda x: x[1])
            return [x[0] for x in temp_rankings]

    def add_weekly_ranking(
        self,
        team_name,
        team_abbr,
        year,
        week,
        margin_of_victory,
        strength_of_schedule,
        offense_srs,
        defense_srs,
    ):
        team_id = self.get_team_id(team_abbr)
        try:
            logger.info(
                f"Adding new ranking year={year} week={week} team_id={team_id} margin_of_victory={margin_of_victory} strength_of_schedule={strength_of_schedule} offense={offense_srs} defense={defense_srs}"
            )
            self.cur.execute(
                "INSERT INTO weekly_rankings (team_id,year,week,margin_of_victory,strength_of_schedule,offense_srs,defense_srs) VALUES (?,?,?,?,?,?,?)",
                (
                    team_id,
                    year,
                    week,
                    margin_of_victory,
                    strength_of_schedule,
                    offense_srs,
                    defense_srs,
                ),
            )
        except sqlite3.Error as error:
            logger.exception(
                f"Error adding ranking for week {week} year {year} team {team_id} offense {offense_srs} defense {defense_srs} || {error}"
            )
        else:
            self.conn.commit()
            logger.info(f"Successfully added weekly stats, calculating actual ranking")
            try:
                offense_rankings = self.get_ranked_rankings(week, year, is_offense=True)
                defense_rankings = self.get_ranked_rankings(
                    week, year, is_offense=False
                )
                offense_ranking = offense_rankings.index(team_id)
                defense_ranking = defense_rankings.index(team_id)
                self.cur.execute(
                    "Update weekly_ranking SET offense_rank = ?, defense_rank=? WHERE team_id = ? AND week = ? AND year = ?",
                    (offense_ranking, defense_ranking, team_id, week, year),
                )
            except sqlite3.Error as error:
                logger.exception(
                    f"Failed to update offense and defense ranking for team {team_id} week {week} year {year} || {error}"
                )
            else:
                self.conn.commit()

    def add_weekly_stats(self, player, year, week, stats):
        player_id = self.get_player_id(player)

        for stat_name, stat_value in stats.items():
            logger.info(
                f"Adding weekly {stat_name} as {stat_value} to {player} for {year} week {week}"
            )
            weekly_exists = self.cur.execute(
                "SELECT count(*) FROM weekly_stats WHERE year = ? AND week = ? AND player_id = ?",
                (year, week, player_id),
            )
            weekly_exists = weekly_exists.fetchone()["count(*)"]
            if weekly_exists:
                try:
                    sql = f"UPDATE weekly_stats SET {stat_name} = ? WHERE player_id = ? AND year = ? AND week = ?"
                    self.cur.execute(sql, (stat_value, player_id, year, week))
                except sqlite3.Error as error:
                    logger.exception(
                        f"Error updating {stat_name} for week {week} of {year} for {player} || {error}"
                    )
                else:
                    self.conn.commit()
            else:

                sql = f"INSERT INTO weekly_stats (year,week,player_id,{stat_name}) VALUES (?,?,?,?)"
                try:
                    self.cur.execute(
                        sql,
                        (year, week, player_id, stat_value),
                    )
                except sqlite3.Error as error:
                    logger.exception(
                        f"Error adding weekly stat {stat_name} to {stat_value} for {player} on week {week} of {year} || {error}"
                    )
                else:
                    self.conn.commit()

    def add_yearly_stats(self, player, year, stats):
        player_id = self.get_player_id(player)

        for stat_name, stat_value in stats.items():
            logger.info(
                f"Adding {stat_name} as {stat_value} to {player} for year {year}"
            )
            yearly_exists = self.cur.execute(
                "SELECT count(*) FROM yearly_stats WHERE year = ? AND player_id = ?",
                (year, player_id),
            )
            yearly_exists = yearly_exists.fetchone()["count(*)"]
            if yearly_exists:
                try:
                    sql = f"UPDATE yearly_stats SET {stat_name} = ? WHERE player_id = ? AND year = ?"
                    self.cur.execute(sql, (stat_value, player_id, year))
                except sqlite3.Error as error:
                    logger.exception(
                        f"Error updating {stat_name} for {year} for {player} || {error}"
                    )
                else:
                    self.conn.commit()
            else:

                sql = f"INSERT INTO yearly_stats (year,player_id,{stat_name}) VALUES (?,?,?)"
                try:
                    self.cur.execute(
                        sql,
                        (year, player_id, stat_value),
                    )
                except sqlite3.Error as error:
                    logger.exception(
                        f"Error adding yearly stat {stat_name} to {stat_value} for {player} of {year} || {error}"
                    )
                else:
                    self.conn.commit()

    def get_team_id(self, team_abbr):
        logger.info(f"Getting team id for {team_abbr}")
        try:
            team_id_data = self.cur.execute(
                "SELECT team_id FROM teams WHERE team_name_abbr = ?", (team_abbr,)
            )
        except sqlite3.Error as error:
            logger.exception(f"Error getting team id for {team_abbr} | {error}")
            return -1
        else:
            team_id = team_id_data.fetchone()["team_id"]
            return team_id
