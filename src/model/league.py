from utilities.connections.baseball_data_connection import DatabaseConnection
from model.properties import sandbox_mode


class League:

    def __init__(self, home_team, year):
        self.home_team = home_team
        self.year = year
        self.league = self.retrieve_league()

    def retrieve_league(self):
        db = DatabaseConnection(sandbox_mode)
        league = db.read('select league from team_years where teamid = ' + self.home_team + ' and year = '
                         + str(self.year) + ';')
        db.close()
        return league

    def get_league(self):
        return self.league
