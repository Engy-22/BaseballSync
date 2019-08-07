import random
import os
import time
from model.plate_appearance import PlateAppearance
from controller.pitch import simulate_pitch
from utilities.logger import Logger
from utilities.properties import log_prefix
from utilities.time_converter import time_converter

logger = Logger(os.path.join(log_prefix, "controller", "plate_appearance.log"))


def simulate_plate_appearance(batting_info, pitching_info, lineup, place, pitcher, inning, driver_logger):
    driver_logger.log('\t\tPlate appearance ' + lineup[place].get_full_name() + " vs. " + pitcher.get_full_name())
    start_time = time.time()
    logger.log("Simulating plate appearance: " + lineup[place].get_full_name() + " vs. " + pitcher.get_full_name())
    batter = lineup[place]
    if not batter.get_batting_stats():
        batter.set_batting_stats(batting_info[batter.get_player_id()])
    if not pitcher.get_pitching_stats():
        pitcher.set_pitching_stats(pitching_info[pitcher.get_player_id()])
    plate_appearance_data = {'increment_batter': True}
    plate_appearance = PlateAppearance(batter, pitcher)
    batter_orientation = batter_handedness(batter, pitcher)
    pitcher_orientation = pitcher_handedness(pitcher)
    while plate_appearance.get_outcome() is None:
        pitch_data = simulate_pitch(pitcher, batter, batter_orientation, pitcher_orientation,
                                    plate_appearance.get_balls(), plate_appearance.get_strikes(), logger)
        if pitch_data['pa_completed']:
            plate_appearance.set_outcome(pitch_data['outcome'])
        else:
            if pitch_data['outcome'] == 'ball':
                plate_appearance.increment_balls()
            else:
                plate_appearance.increment_strikes()
################### random data ###################
    plate_appearance_data['runs'] = random.randint(0, 1)
    plate_appearance_data['hits'] = random.randint(0, 1)
    plate_appearance_data['errors'] = random.randint(0, 1)
    plate_appearance_data['lineup'] = lineup
    plate_appearance_data['pitcher'] = pitcher
################### random data ###################
    plate_appearance_data['outs'] = calculate_outs(plate_appearance.get_outcome())
    total_time = time_converter(time.time() - start_time)
    logger.log('\tOutcome: ' + plate_appearance.get_outcome())
    logger.log("Done simulating plate appearance: Time = " + total_time + '\n\n')
    driver_logger.log('\t\tOutcome: ' + plate_appearance.get_outcome())
    driver_logger.log("\t\t\tTime = " + total_time)
    return plate_appearance_data


def calculate_outs(outcome):
    if outcome == "BB":
        return 0
    elif "K " in outcome:
        return 1


def batter_handedness(batter, pitcher):
    if not batter.get_batting_handedness():
        batter.set_batting_handedness()
    if not pitcher.get_throwing_handedness():
        pitcher.set_throwing_handedness()
    batter_orientation = batter.get_batting_handedness()
    if batter_orientation == 'B':
        batter_orientation = 'L' if pitcher.get_throwing_handedness() == 'R' else 'R'
    return 'v' + batter_orientation.lower()


def pitcher_handedness(pitcher):
    if not pitcher.get_throwing_handedness():
        pitcher.set_throwing_handedness()
    pitcher_orientation = pitcher.get_throwing_handedness()
    return 'v' + pitcher_orientation.lower()


# from model.players.player import Player
# from model.teams.team import Team
# mets = Team('NYM', 2017)
# indians = Team('CLE', 2017)
# lindor = Player('lindofr01', 'CLE', 2017)
# lindor.set_full_name()
# degrom = Player('degroja01', 'NYM', 2017)
# degrom.set_full_name()
# simulate_plate_appearance(indians.get_team_info()['batter_stats'], mets.get_team_info()['pitcher_stats'], [lindor], 0,
#                           degrom, 1, Logger('C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\'
#                                             'sandbox\\controller\\inning.log'))
