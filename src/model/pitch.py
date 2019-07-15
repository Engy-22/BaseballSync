class Pitch:

    def __init__(self, pitcher, pitch_type, balls, strikes):
        self.pitcher = pitcher
        self.pitch_type = pitch_type
        self.balls = balls
        self.strikes = strikes
        self.pitch_type = ''
        self.strike = ''  # bool

### RETRIEVERS ###
### END RETRIEVERS ###

### SETTERS ###
    def set_pitch_type(self, pitch_type):
        self.pitch_type = pitch_type
### END SETTERS ###

### GETTERS ###
    def get_pitch_type(self):
        return self.pitch_type
### END GETTERS ###
