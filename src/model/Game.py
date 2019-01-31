class Game:

    def __init__(self, away_team, home_team):
        self.away_team = away_team
        self.home_team = home_team
        self.away_score = 0
        self.home_score = 0

    def increment_away_score(self, added_runs):
        self.away_score += added_runs

    def increment_home_score(self, added_runs):
        self.home_score += added_runs

############ Getters ############
    def get_home_score(self):
        return self.home_score

    def get_away_score(self):
        return self.away_score

    def get_away_team(self):
        return self.away_team

    def get_home_team(self):
        return self.home_team
############ Getters ############