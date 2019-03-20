import os
from model.plate_appearance import PlateAppearance
from controller.pitch import simulate_pitch
import time
from utilities.logger import Logger
from utilities.time_converter import time_converter

logger = Logger(os.path.join("..", "..", "logs", "controller", "plate_appearance.log"))


def simulate_plate_appearance(batter_id, pitcher_id, driver_logger):
    driver_logger.log('\t\tPlate appearance' + batter_id + " vs. " + pitcher_id)
    start_time = time.time()
    logger.log("Simulating plate appearance: " + batter_id + " vs. " + pitcher_id)
    plate_appearance_data = {}
    plate_appearance = PlateAppearance(batter_id, pitcher_id)
    outcome = None
    while plate_appearance.get_balls() < 4 and plate_appearance.get_strikes() < 3 and outcome is None:
        pitch_data = simulate_pitch(plate_appearance.get_balls(), plate_appearance.get_strikes(), logger)
    total_time = time_converter(time.time() - start_time)
    logger.log("Done simulating plate appearance: Time = " + total_time + '\n\n')
    driver_logger.log("\t\t\tTime = " + total_time)
    return plate_appearance_data
