from utilities.connections.pitchers_pitch_fx_connection import PitcherPitchFXDatabaseConnection as db
from model.properties import sandbox_mode


class Pitch:

    def __init__(self, pitcher, balls, strikes):
        self.pitcher = pitcher
        self.balls = balls
        self.strikes = strikes
        self.pitch_type = ''
        self.strike = ''  # bool

### RETRIEVERS ###
### END RETRIEVERS ###

### SETTERS ###
### END SETTERS ###

### GETTERS ###
### END GETTERS ###
