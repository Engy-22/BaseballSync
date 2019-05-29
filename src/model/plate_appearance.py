class PlateAppearance:

    def __init__(self, batter, pitcher):
        self.batter = batter
        self.pitcher = pitcher
        self.balls = 0
        self.strikes = 0
        self.outcome = None

### RETRIEVERS ###
### END RETRIEVERS ###

### SETTERS ###
    def increment_balls(self):
        self.balls += 1

    def increment_strikes(self):
        self.strikes += 1

    def set_outcome(self, outcome):
        self.outcome = outcome
### END SETTERS ###

### GETTERS ###
    def get_balls(self):
        return self.balls

    def get_strikes(self):
        return self.strikes

    def get_outcome(self):
        return self.outcome
### END GETTERS ###
