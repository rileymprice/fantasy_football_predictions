from log_helper import Logger

logger = Logger(__name__)


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


class Database:
    def __init__(self, db):
        self.db = db
        self.conn = sqlite3.connect(db)
        self.conn.row_factory = dict_factory
        self.cur = self.conn.cursor()
        self.cur.execute(
            """CREATE TABLE IF NOT EXISTS players (
                                    player_id    INTEGER PRIMARY KEY AUTOINCREMENT,
                                    full_name    VARCHAR,
                                    birth_date   DATE,
                                    espn_id      INT,
                                    pfr_url_name VARCHAR,
                                    search_name  VARCHAR
                                );"""
        )
        self.cur.execute(
            """CREATE TABLE IF NOT EXISTS teams (
                            team_id        INTEGER PRIMARY KEY AUTOINCREMENT,
                            team_name      VARCHAR,
                            team_name_abbr VARCHAR
                        );"""
        )
        self.cur.execute(
            """CREATE TABLE IF NOT EXISTS schedule (
                            schedule_id  INTEGER PRIMARY KEY AUTOINCREMENT,
                            week         INT     NOT NULL,
                            home_team_id INT     REFERENCES teams (team_id),
                            away_team_id INT     REFERENCES teams (team_id),
                            year         INT     NOT NULL,
                            date         DATE
                        );"""
        )
        self.cur.execute(
            """CREATE TABLE IF NOT EXISTS player_info (
                            info_id   INTEGER PRIMARY KEY AUTOINCREMENT,
                            player_id INT     REFERENCES players (player_id),
                            year      INT,
                            height    INT,
                            weight    INT,
                            age       INT,
                            team_id   INT     REFERENCES teams (team_id)
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
            """CREATE TABLE IF NOT EXISTS weekly_stats (
                                        player_id        INT     PRIMARY KEY
                                                                REFERENCES players (player_id),
                                        week             INT     DEFAULT (0),
                                        pass_yards       INT     DEFAULT (0),
                                        pass_attempts    INT     DEFAULT (0),
                                        pass_completions INT     DEFAULT (0),
                                        pass_int         INT     DEFAULT (0),
                                        pass_td          INT     DEFAULT (0),
                                        sack             INT     DEFAULT (0),
                                        rush_yards       INT     DEFAULT (0),
                                        rush_attempts    INT     DEFAULT (0),
                                        fumbles          INT     DEFAULT (0),
                                        fumbles_lost     INT     DEFAULT (0),
                                        rec_yards        INT     DEFAULT (0),
                                        rec_targets      INT     DEFAULT (0),
                                        receptions       INT     DEFAULT (0),
                                        year             INT     NOT NULL
                                                                DEFAULT (0),
                                        snap_percentage  INT     DEFAULT (0),
                                        fantasy_points   DOUBLE  DEFAULT (0),
                                        start            BOOLEAN DEFAULT (0),
                                        pass_rating      DOUBLE  DEFAULT (0),
                                        injured          BOOLEAN DEFAULT (0),
                                        team_id          INT     REFERENCES teams (team_id) 
                                    );"""
        )

    def create_player(
        self, name, search_name, birth_date, team, pfr_url_name, espn_id=00000
    ):
        try:
            logger.info(
                f"Adding player name={name} search_name={search_name} birthdate={birth_date} team={team} pfr_url={pfr_url_name} espn_id={espn_id} "
            )
            self.cur.execute(
                "INSERT INTO players (name,search_name,birth_date,team,pfr_url_name,espn_id) VALUES (?,?,?,?,?,?)",
                (
                    name,
                    search_name,
                    birth_date,
                    team,
                    pfr_url_name,
                    espn_id,
                ),
            )
        except sqlite3.Error as error:
            logger.error(f"Error adding player {name}|| {error}")
        else:
            self.conn.commit()
            logger.info(f"Successfully added {name} to players DB")

    def create_team(self, team_name, team_abbr):
        try:
            logger.info(f"Adding team team_name={team_name} team_abbr={team_abbr}")
            self.cur.execute(
                "INTO INTO teams (team_name,team_abbr) VALUES (?,?)",
                (team_name, team_abbr),
            )
        except sqlite3.Error as error:
            logger.error(f"Error adding team {team_name}|| {error}")
        else:
            self.conn.commit()
            logger.info(f"Successfully added {team_name}")

    def add_schedule(self, home_team_id, away_team_id, date, week):
        try:
            logger.info(
                f"Adding new schedule date={date} week={week} home_team_id={home_team_id} away_team_id={away_team_id}"
            )
            self.cur.execute(
                "INSERT INTO schedule (home_team_id,away_team_id,date,week) VALUES (?,?,?,?)",
                (home_team_id, away_team_id, date, week),
            )
        except sqlite3.Error as error:
            logger.error(
                f"Error adding schedule for week {week} home_team {home_team_id} away_team_id {away_team_id}"
            )
        else:
            self.conn.commit()
            logger.info(
                f"Successfully created schedule for {week} home={home_team_id} away={away_team_id}"
            )

    def add_weekly_ranking(
        self,
        team_id,
        year,
        week,
        margin_of_victory,
        strength_of_schedule,
        offense_srs,
        defense_srs,
    ):
        try:
            logger.info(
                f"Adding new ranking year={year} week={week} team_id={team_id} margin_of_victory={margin_of_year} strength_of_schedule={strength_of_schedule} offense={offense} defense={defense}"
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
            logger.error(
                f"Error adding ranking for week {week} year {year} team {team_id} offense {offense} defense {defense} || {error}"
            )
        else:
            self.conn.commit()
            logger.info(f"Successfully added weekly stats, calculating actual ranking")
            try:
                offense_rankings = get_ranked_rankings(is_offense = True,week,year)
                defense_rankings = get_ranked_rankings(is_offense = False,week,year)
                offense_ranking = offense_rankings.index(team_id)
                defense_ranking = defense_rankings.index(team_id)
                self.cur.execute('Update weekly_ranking SET offense_rank = ?, defense_rank=? WHERE team_id = ? AND week = ? AND year = ?',(offense_ranking,defense_ranking,team_id,week,year))
            except sqlite3.Error as error:
                logger.error(f'Failed to update offense and defense ranking for team {team_id} week {week} year {year} || {error}')
            else:
                self.conn.commit()

    def get_ranked_rankings(self, is_offense, week, year):
        team_type = 'offense' if is_offense = True else 'defense'
        try:
            logger.info('Getting ranked rankings')
            rankings = self.cur.execute('SELECT team_id, ?_srs FROM weekly_rankings WHERE week = ? AND year = year', (team_type,week,year))
        except sqlite3.Error as error:
            logger.error(f'Failed getting ranked rankings for week {week} year {year}')
        else:
            logger.info(f'Retrieved ranked rankings for week {week} on year {year}')
            rankings = rankings.fetchall()
            temp_rankings = []
            for ranking in rankings:
                temp_rankings.append((ranking['team_id'],ranking[f'{team_type}_srs']))
            temp_rankings.sort(key=lambda x:x[1])
            return [x[0] for x in temp_rankings]

    def add_weekly_stats(self, player_id, year, week, **stats):
        for stat_name, stat_value in stats.items():
            try:
                logger.info(f"Adding {stat_name} as {stat_value} to {player_id}")
                self.cur.execute(
                    f"UPDATE weekly_stats SET year = ?, week = ?, ? = ? WHERE player_id = ?)",
                    (year, week, stat_name, stat_value, player_id),
                )
            except sqlite3.Error as error:
                logger.error(
                    f"Error adding stat {stat_name} to {stat_value} for {player_id} on week {week} of {year} || {error}"
                )
            else:
                self.conn.commit()
                logger.info(
                    f"Successfully updated {stat_name} to {stat_value} for {player_id}"
                )
