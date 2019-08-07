from model.players.player import Player


class Pitcher(Player):

    def __init__(self, player_id: str, team_id: str, year: int):
        super().__init__(player_id, team_id, year)
        self.stats = None
        self.pitch_count = 0

    def set_pitching_stats(self, pitching_stats):
        self.stats = pitching_stats

    def get_pitching_stats(self):
        return self.stats
