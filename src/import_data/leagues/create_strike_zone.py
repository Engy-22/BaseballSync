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
    points = {}
    db = PitchFXDatabaseConnection(sandbox_mode)
    x_strikes = [x[0] for x in db.read('select x from pitcher_pitches where x is not NULL and ball_strike = "strike" '
                                       'and swing_take = "take";')]
    x_strikes.sort()
    y_strikes = [y[0] for y in db.read('select y from pitcher_pitches where y is not NULL and ball_strike = "strike" '
                                       'and swing_take = "take";')]
    y_strikes.sort()
    passes = 1
    for median, coordinates in {stat.median(x_strikes): x_strikes, stat.median(y_strikes): y_strikes}.items():
        coordinate_orientation = 'x' if passes == 1 else 'y'
        threshold = 1000
        for direction in ['positive', 'negative']:
            sparse_intervals = 0
            if direction == 'positive':
                incrementer = 1
            else:
                incrementer = -1
            place_on_number_line = median + incrementer
            while sparse_intervals < 3:  # a sparse interval is an interval with less than a given number of data points
                if coordinates.count(place_on_number_line) < threshold:
                    sparse_intervals += 1  # increment the number of sparse interval
                else:
                    sparse_intervals = 0  # reset the number of sparse intervals
                place_on_number_line += incrementer  # move farther from the median in the appropriate direction
            points[coordinate_orientation + ('_high' if direction == 'positive' else '_low')] =\
                place_on_number_line - (incrementer * 3)
        points[coordinate_orientation + '_middle'] = \
            (points[coordinate_orientation + '_low'] + points[coordinate_orientation + '_high']) / 2
        for meridian, multiplier in {'_meridian_1': 1, '_meridian_2': 2}.items():
            points[coordinate_orientation + meridian] = points[coordinate_orientation + '_low'] + \
                ((points[coordinate_orientation + '_high'] - points[coordinate_orientation + '_low']) / 3) * multiplier
        passes += 1
    db.close()
    try:
        with open(os.path.join("..", "background", "strike_zone.json"), "w") as strike_zone_file:
            json.dump(points, strike_zone_file)
    except FileNotFoundError:
        with open(os.path.join("..", "..", "..", "background", "strike_zone.json"), "w") as strike_zone_file:
            json.dump(points, strike_zone_file)
    driver_logger.log('\t\tTime = ' + time_converter(time.time() - start_time))


# create_strike_zone()
