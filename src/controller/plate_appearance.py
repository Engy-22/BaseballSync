import random
import os
import time
from model.plate_appearance import PlateAppearance
from controller.pitch import simulate_pitch
from utilities.logger import Logger
from utilities.properties import log_prefix
from utilities.time_converter import time_converter

logger = Logger(os.path.join(log_prefix, "controller", "plate_appearance.log"))


def simulate_plate_appearance(lineup, place, pitcher, inning, driver_logger):
    driver_logger.log('\t\tPlate appearance ' + lineup[place].get_full_name() + " vs. " + pitcher.get_full_name())
    start_time = time.time()
    logger.log("Simulating plate appearance: " + lineup[place].get_full_name() + " vs. " + pitcher.get_full_name())
    plate_appearance_data = {'increment_batter': True}
    plate_appearance = PlateAppearance(lineup[place], pitcher)
    while plate_appearance.get_balls() < 4 and plate_appearance.get_strikes() < 3 and plate_appearance.get_outcome() is None:
        pitch_data = simulate_pitch(pitcher, lineup[place], plate_appearance.get_balls(), plate_appearance.get_strikes(),
                                    inning, logger)
################### random data ###################
        if pitch_data['ball_strike'] == 'ball':
            plate_appearance.increment_balls()
        else:
            plate_appearance.increment_strikes()
    plate_appearance_data['runs'] = random.randint(0, 3)
    plate_appearance_data['hits'] = random.randint(0, 3)
    plate_appearance_data['errors'] = random.randint(0, 1)
    plate_appearance_data['lineup'] = lineup
    plate_appearance_data['pitcher'] = pitcher
################### random data ###################
    plate_appearance_data['outs'] = calculate_outs(plate_appearance.get_outcome())
    total_time = time_converter(time.time() - start_time)
    logger.log("Done simulating plate appearance: Time = " + total_time + '\n\n')
    driver_logger.log("\t\t\tTime = " + total_time)
    return plate_appearance_data


def calculate_outs(outcome):
    if outcome is None:
        return 1
