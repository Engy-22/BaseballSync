class Inning:
    def __init__(self):
        self.outs = 0
        self.away_runs = 0
        self.home_runs = 0

    def increment_outs(self):
        self.outs += 1

################### Getters ###################
    def get_away_runs(self):
        return self.away_runs

    def get_home_runs(self):
        return self.home_runs

    def get_outs(self):
        return self.outs
################### Getters ###################
