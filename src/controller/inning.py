from model.inning import Inning
from utilities.logger import Logger
from controller.plate_appearance import simulate_plate_appearance
import time
from utilities.time_converter import time_converter

logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\controller\\inning.log")


def simulate_inning(game, driver_logger):
    inning_num = str(game.get_inning())
    driver_logger.log('\tInning ' + inning_num)
    start_time = time.time()
    inning = Inning()
    inning_data = {}
    logger.log("Starting inning simulation: " + game.get_away_team() + " @ " + game.get_home_team() + " - " + inning_num)
    for half in ['top', 'bottom']:
        inning.set_half_inning(half)
        half_inning = inning.get_half_inning()
        for key, value in simulate_half_inning(game, inning).items():
            inning_data[half_inning] = ''  # put the half inning data into the inning data dictionary
    game.increment_inning()
    total_time = time_converter(time.time() - start_time)
    driver_logger.log('\t\tTime = ' + total_time)
    logger.log("Done simulating inning: " + game.get_away_team() + " @ " + game.get_home_team() + " - " + inning_num
               + ": Time = " + total_time + '\n\n')
    return inning_data


def simulate_half_inning(game, inning):
    start_time = time.time()
    inning_num = game.get_inning
    half_inning = inning.get_half_inning()
    if inning_num == '9' and half_inning == 'bottom' and game.home_score > game.away_score:
        return
    logger.log('\tSimulating the ' + half_inning + ' of the ' + inning_num + ' inning')
    half_inning_data = {}
    outs = 0
    while outs < 3:
        plate_appearance_data = simulate_plate_appearance('', '', logger)
    logger.log('\tDone simulating the ' + half_inning + 'of the ' + inning_num + ' inning: Time = '
               + time_converter(time.time() - start_time))
    return half_inning_data
