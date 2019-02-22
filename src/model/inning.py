class Inning:
    def __init__(self):
        self.outs = 0
        self.away_runs = 0
        self.home_runs = 0
        self.half_inning = ''

### RETRIEVERS ###
### END RETRIEVERS ###

### SETTERS ###
    def set_half_inning(self, half):
        self.half_inning = half

    def increment_outs(self):
        self.outs += 1
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
### END GETTERS ###
