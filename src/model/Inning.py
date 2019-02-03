class Inning:
    def __init__(self):
        self.outs = 0
        self.away_runs = 0
        self.home_runs = 0
        self.half_inning = ''

    def switch_half_inning(self):
        if self.half_inning == 'top':
            self.half_inning = 'bottom'
        else:
            self.half_inning = 'top'

    def increment_outs(self):
        self.outs += 1

################### Getters ###################
    def get_away_runs(self):
        return self.away_runs

    def get_home_runs(self):
        return self.home_runs

    def get_outs(self):
        return self.outs

    def get_half_inning(self):
        return self.half_inning
################### Getters ###################
