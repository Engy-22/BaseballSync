from model.Inning import Inning
from utilities.logger import Logger
from controller.plate_appearance import simulate_plate_appearance
import time
from utilities.time_converter import time_converter

logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\controller\\game.log")


def simulate_inning(game, driver_logger):
    inning = Inning()
    inning_num = str(game.get_inning())
    driver_logger.log('\tInning ' + inning_num)
    start_time = time.time()
    logger.log("Starting inning simulation: " + game.get_away_team() + " @ " + game.get_home_team() + " - " + inning_num)
    for half in range(2):
        inning.switch_half_inning()
        simulate_half_inning(game, inning_num, inning.get_half_inning())
    total_time = time_converter(time.time() - start_time)
    driver_logger.log('\t\tTime = ' + total_time)
    logger.log("Done simulating inning: " + game.get_away_team() + " @ " + game.get_home_team() + " - " + inning_num
               + ": Time = " + total_time + '\n\n')


def simulate_half_inning(game, inning, half):
    start_time = time.time()
    if inning == '9' and half == 'bottom' and game.home_score > game.away_score:
        return
    logger.log('\tSimulating the ' + half + ' of the ' + inning + ' inning')
    outs = 0
    while outs < 3:
        simulate_plate_appearance('', '', logger)
    logger.log('\tDone simulating the ' + half + 'of the ' + inning + ' inning: Time = '
               + time_converter(time.time() - start_time))
