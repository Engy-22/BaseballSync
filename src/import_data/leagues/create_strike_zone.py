import statistics as stat
from utilities.database.wrappers.pitch_fx_connection import PitchFXDatabaseConnection
from utilities.properties import sandbox_mode


def create_strike_zone():
    db = PitchFXDatabaseConnection(sandbox_mode)
    x_points = db.read('select x from pitcher_pitches where x and ball_strike = "strike" and swing_take = "take";')
    y_points = db.read('select y from pitcher_pitches where y and ball_strike = "strike" and swing_take = "take";')
    db.close()
    x_coordinates = [int(x[0]) for x in x_points]
    y_coordinates = [int(y[0]) for y in y_points]
    x_stdev = stat.stdev(x_coordinates)
    y_stdev = stat.stdev(y_coordinates)
    x_mean = stat.mean(x_coordinates)
    y_mean = stat.mean(y_coordinates)
