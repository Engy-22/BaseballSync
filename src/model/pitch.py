class Pitch:

    def __init__(self, pitcher, pitch_type, balls, strikes):
        self.pitcher = pitcher
        self.pitch_type = pitch_type
        self.balls = balls
        self.strikes = strikes
        self.strike = ''  # bool

### RETRIEVERS ###
### END RETRIEVERS ###

### SETTERS ###
### END SETTERS ###

### GETTERS ###
    def get_pitch_type(self):
        return self.pitch_type
### END GETTERS ###
