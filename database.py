from log_helper import Logger

logger = Logger(__name__)

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


class Database():
    def __init__(self, db):
        self.db = db
        self.conn = sqlite3.connect(db)
        self.conn.row_factory = dict_factory
        self.cur = self.conn.cursor()
        self.cur.execute("""CREATE TABLE IF NOT EXISTS players (
                            player_id    INTEGER PRIMARY KEY AUTOINCREMENT,
                            full_name    VARCHAR,
                            birth_date   DATE,
                            espn_id      INT,
                            pos          VARCHAR,
                            pfr_url_name VARCHAR
                        );""")
        self.cur.execute("""CREATE TABLE IF NOT EXISTS teams (
                            team_id        INTEGER PRIMARY KEY AUTOINCREMENT,
                            team_name      VARCHAR,
                            team_name_abbr VARCHAR
                        );""")
        self.cur.execute("""CREATE TABLE IF NOT EXISTS schedule (
                            schedule_id  INTEGER PRIMARY KEY AUTOINCREMENT,
                            week         INT     NOT NULL,
                            home_team_id INT     REFERENCES teams (team_id),
                            away_team_id INT     REFERENCES teams (team_id),
                            year         INT     NOT NULL,
                            date         DATE
                        );""")
        self.cur.execute("""CREATE TABLE player_info (
                            info_id   INTEGER PRIMARY KEY AUTOINCREMENT,
                            player_id INT     REFERENCES players (player_id),
                            year      INT,
                            height    INT,
                            weight    INT,
                            age       INT,
                            team_id   INT     REFERENCES teams (team_id) 
                        );""")
        self.cur.execute("""CREATE TABLE weekly_rankings (
                            team_id      INT PRIMARY KEY REFERENCES teams (team_id),
                            week         INT,
                            offense_rank INT,
                            defense_rank INT,
                            year         INT
                        );""")
        self.cur.execute("""CREATE TABLE weekly_stats (
                            player_id        INT     PRIMARY KEY REFERENCES players (player_id),
                            team_id          INT     REFERENCES teams (team_id),
                            week             INT,
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
                            year             INT     NOT NULL,
                            snap_percentage  INT     DEFAULT (0),
                            fantasy_points   DOUBLE  DEFAULT (0),
                            start            BOOLEAN DEFAULT (0),
                            pass_rating      DOUBLE,
                            injured          BOOLEAN DEFAULT (0)
                        );""")


    def create_player(self,name,birth_date,team,pfr_url_name,espn_id=00000):
        try:
            logger.info(
                f"Adding player name={name} birthdate={birth_date} team={team} pfr_url={pfr_url_name} espn_id={espn_id} "
            )
            self.cur.execute(
                "INSERT INTO players (name,birth_date,team,pfr_url_name,espn_id) VALUES (?,?,?,?,?)",
                (name, birth_date, team, pfr_url_name, espn_id,),
            )
        except sqlite3.Error as error:
            logger.error(f"Error adding player {name}|| {error}")
        else:
            self.conn.commit()
            logger.info(f"Successfully added {name} to players DB")

    def create_team(self,team_name,team_abbr):
        try:
            logger.info(f'Adding team team_name={team_name} team_abbr={team_abbr}')
            self.cur.execute("INTO INTO teams (team_name,team_abbr) VALUES (?,?)",(team_name,team_abbr))
        except sqlite3.Error as error:
            logger.error(f'Error adding team {team_name}|| {error}')
        else:
            self.conn.commit()
            logger.info(f'Successfully added {team_name}')

    def add_schedule(self,home_team_id,away_team_id,date,week):
        try:
            logger.info(f'Adding new schedule date={date} week={week} home_team_id={home_team_id} away_team_id={away_team_id}')
            self.cur.execute('INSERT INTO schedule (home_team_id,away_team_id,date,week) VALUES (?,?,?,?)',(home_team_id,away_team_id,date,week))
        except sqlite3.Error as error:
            logger.error(f'Error adding schedule for week {week} home_team {home_team_id} away_team_id {away_team_id}')
        else:
            self.conn.commit()
            logger.info(f'Successfully created schedule for {week} home={home_team_id} away={away_team_id}')

    def add_weekly_ranking(self,team_id,year,week,offense,defense):
        try:
            logger.info(f'Adding new ranking year={year} week={week} team_id={team_id} offense={offense} defense={defense}')
            self.cur.execute('INSERT INTO weekly_rankings (team_id,year,week,offense_rank,defense_rank) VALUES (?,?,?,?,?)',(team_id,year,week,offense,defense))
        except sqlite3.Error as error:
            logger.error(f'Error adding ranking for week {week} year {year} team {team_id} offense {offense} defense {defense}')
        else:
            self.conn.commit()
            logger.info(f'Successfully added ranking')
            
    def add_weekly_stats(self,player_id)