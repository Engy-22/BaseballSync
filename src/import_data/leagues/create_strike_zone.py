import os
import json
import time
import statistics as stat
from utilities.database.wrappers.pitch_fx_connection import PitchFXDatabaseConnection
from utilities.properties import sandbox_mode, import_driver_logger as driver_logger
from utilities.time_converter import time_converter


def create_strike_zone():
    start_time = time.time()
    driver_logger.log('\tCreating Strike Zone')
    db = PitchFXDatabaseConnection(sandbox_mode)
    x_points = db.read('select x from pitcher_pitches where x is not NULL and ball_strike = "strike" and swing_take '
                       '= "take";')
    y_points = db.read('select y from pitcher_pitches where y is not NULL and ball_strike = "strike" and swing_take '
                       '= "take";')
    db.close()
    points = {}
    x_coordinates = [int(x[0]) for x in x_points]
    y_coordinates = [int(y[0]) for y in y_points]
    x_stdev = stat.stdev(x_coordinates)
    y_stdev = stat.stdev(y_coordinates)
    points['x_middle'] = stat.mean(x_coordinates)
    points['y_middle'] = stat.mean(y_coordinates)
    points['x_low'] = points['x_middle'] - (3 * x_stdev)
    points['x_high'] = points['x_middle'] + (3 * x_stdev)
    points['y_low'] = points['y_middle'] - (3 * y_stdev)
    points['y_high'] = points['y_middle'] + (3 * y_stdev)
    try:
        with open(os.path.join("..", "background", "strike_zone.json"), "w") as strike_zone_file:
            json.dump(points, strike_zone_file)
    except FileNotFoundError:
        with open(os.path.join("..", "..", "..", "background", "strike_zone.json"), "w") as strike_zone_file:
            json.dump(points, strike_zone_file)
    driver_logger.log('\t\tTime = ' + time_converter(time.time() - start_time))


# create_strike_zone()
