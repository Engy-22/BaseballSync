from utilities.dbconnect import DatabaseConnection


class League:

    def __init__(self, home_team, year):
        self.home_team = home_team
        self.year = year
        self.league = ''

    def set_league(self):
        db = DatabaseConnection()
        self.league = db.read('select league from team_years where teamid = ' + self.home_team + ' and year = '
                              + str(self.year) + ';')
        db.close()

    def get_league(self):
        return self.league
