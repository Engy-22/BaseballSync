from model.players.player import Player
from ast import literal_eval
from utilities.database.wrappers.baseball_data_connection import DatabaseConnection


class Batter(Player):

    def __init__(self, player_id: str, team_id: str, year: int):
        super().__init__(player_id, team_id, year)
        self.stats = None
        self.bats_with = None
        self.batting_spots = {}
        self.simulation_stats = {}

    def set_batting_spots(self, batting_spots):
        self.batting_spots = batting_spots

    def set_batting_stats(self, batting_stats, pitchers_batting_stats):
        if batting_stats:
            self.stats = batting_stats
        else:
            self.stats = pitchers_batting_stats

    def set_batting_handedness(self):
        db = DatabaseConnection(sandbox_mode=True)
        self.bats_with = db.read('select batsWith from players where playerId = "' + self.player_id + '";')[0][0]
        db.close()

    def set_simulation_stats(self, stat, value):
        self.simulation_stats[stat] = value

    def get_batting_handedness(self):
        return self.bats_with

    def get_batting_stats(self):
        return self.stats

    def get_batting_spots(self):
        return self.batting_spots

    def get_simulation_stats(self, stat):
        return self.simulation_stats[stat]
