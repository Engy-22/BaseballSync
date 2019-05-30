import os
import time
from model.inning import Inning
from utilities.logger import Logger
from controller.plate_appearance import simulate_plate_appearance
from utilities.time_converter import time_converter

logger = Logger(os.path.join("..", "logs", "sandbox", "controller", "inning.log"))


def simulate_inning(game, lineup, place, pitcher, driver_logger):
    inning_num = str(game.get_inning())
    driver_logger.log('\tInning ' + inning_num)
    start_time = time.time()
    inning = Inning()
    inning_data = {'top': {}, 'bottom': {}}
    logger.log("Starting inning simulation: " + game.get_away_team() + " @ " + game.get_home_team() + " - " + inning_num)
    for half in ['top', 'bottom']:
        inning.set_half_inning(half)
        for key, value in simulate_half_inning(game, inning, lineup[half], place[half], pitcher[half]).items():
            inning_data[half][key] = value  # put the half inning data into the inning data dictionary
    game.increment_inning()
    total_time = time_converter(time.time() - start_time)
    driver_logger.log('\t\tTime = ' + total_time)
    logger.log("Done simulating inning: " + game.get_away_team() + " @ " + game.get_home_team() + " - " + inning_num
               + ": Time = " + total_time + '\n\n')
    return inning_data


def simulate_half_inning(game, inning, lineup, place, pitcher):
    start_time = time.time()
    inning_num = game.get_inning()
    half_inning = inning.get_half_inning()
    logger.log('\tSimulating the ' + str(half_inning) + ' of the ' + str(inning_num) + ' inning')
    half_inning_data = {'runs': 0, 'hits': 0, 'errors': 0, 'lob': 0, 'lineup': lineup, 'pitcher': pitcher, 'place': place}
    while inning.get_outs() < 3:
        if inning_num == '9' and half_inning == 'bottom' and game.get_home_score() > game.get_away_score():
            return half_inning_data
        plate_appearance_data = simulate_plate_appearance(lineup, place, pitcher, inning, logger)
        if plate_appearance_data['increment_batter']:
            place = increment_lineup_place(place)
        inning.increment_outs(plate_appearance_data['outs'])
        half_inning_data['runs'] += plate_appearance_data['runs']
        half_inning_data['hits'] += plate_appearance_data['hits']
        half_inning_data['errors'] += plate_appearance_data['errors']
        lineup = plate_appearance_data['lineup']
        pitcher = plate_appearance_data['pitcher']
    half_inning_data['place'] = place
    half_inning_data['lineup'] = lineup
    half_inning_data['pitcher'] = pitcher
    half_inning_data['lob'] = calculate_runners_lob(inning.get_bases())
    inning.reset_outs()
    logger.log('\tDone simulating the ' + str(half_inning) + ' of the ' + str(inning_num) + ' inning: Time = '
               + time_converter(time.time() - start_time))
    return half_inning_data


def increment_lineup_place(place):
    return place + 1 if place != 8 else 0


def calculate_runners_lob(bases):
    lob = 0
    for base, runner in bases.items():
        if runner is not None:
            lob += 1
    return lob
