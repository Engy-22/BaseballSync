class PlateAppearance:

    def __init__(self, batter, pitcher):
        self.batter = batter
        self.pitcher = pitcher
        self.balls = 0
        self.strikes = 0

### RETRIEVERS ###
### END RETRIEVERS ###

### SETTERS ###
    def increment_balls(self):
        self.balls += 1

    def increment_strikes(self):
        self.strikes += 1
### END SETTERS ###

### GETTERS ###
    def get_balls(self):
        return self.balls

    def get_strikes(self):
        return self.strikes
### END GETTERS ###
