from utilities.DB_Connect import DB_Connect


class League:

    def __init__(self, home_team, year):
        self.home_team = home_team
        self.year = year
        self.league = ''

    def set_league(self, year):
        db, cursor = DB_Connect.grab("baseballData")
        self.league = DB_Connect.read(cursor, 'select league from team_years where teamid = ' + self.home_team
                                              + ' and year = ' + str(self.year) + ';')
        DB_Connect.close(db)

    def get_league(self):
        return self.league
