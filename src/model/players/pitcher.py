from model.players.player import Player


class Pitcher(Player):

    def __init__(self, player_id: str, team_id: str, year: int):
        super().__init__(player_id, team_id, year)
        self.stats = None
        self.pitch_count = 0
        self.energy = 100
        self.simulation_stats = {}

    def set_pitching_stats(self, pitching_stats):
        self.stats = pitching_stats

    def set_simulation_stats(self, stat, value):
        self.simulation_stats[stat] = value

    def get_pitching_stats(self):
        return self.stats

    def increment_pitch_count(self):
        self.pitch_count += 1

    def get_pitch_count(self):
        return self.pitch_count

    def get_simulation_stats(self, stat):
        return self.simulation_stats[stat]
