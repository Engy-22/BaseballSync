class Inning:
    def __init__(self):
        self.outs = 0
        self.away_runs = 0
        self.home_runs = 0
        self.bases = {1: None, 2: None, 3: None}
        self.half_inning = ''

### SETTERS ###
    def reset_outs(self):
        self.outs = 0

    def set_half_inning(self, half):
        self.half_inning = half

    def increment_outs(self, outs):
        self.outs += outs

    def set_bases(self, bases):
        self.bases = bases
### END SETTERS ###

### GETTERS ###
    def get_away_runs(self):
        return self.away_runs

    def get_home_runs(self):
        return self.home_runs

    def get_outs(self):
        return self.outs

    def get_half_inning(self):
        return self.half_inning

    def get_bases(self):
        return self.bases
### END GETTERS ###

    def runners_left_on_base(self):
        return 3 - list(self.bases.values()).count(None)
