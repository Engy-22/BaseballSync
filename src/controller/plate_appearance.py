import random
import os
import time

from model.plate_appearance import PlateAppearance
from controller.pitch import simulate_pitch
from utilities.logger import Logger
from utilities.properties import log_prefix
from utilities.time_converter import time_converter

logger = Logger(os.path.join(log_prefix, "controller", "plate_appearance.log"))


def simulate_plate_appearance(batting_info, pitching_info, batting_year_info, pitching_year_info, lineup, place,
                              pitcher, strike_zone, bases, driver_logger):
    driver_logger.log('\t\tPlate appearance ' + lineup[place].get_full_name() + " vs. " + pitcher.get_full_name())
    start_time = time.time()
    logger.log("Simulating plate appearance: " + lineup[place].get_full_name() + " vs. " + pitcher.get_full_name())

    def wrap_up_plate_appearance(bases):
        plate_appearance.set_outcome(pitch_data['outcome'])
        if pitch_data['outcome'] == 'BB':
            bases, runner_scored = move_runners_up_a_base(bases, batter)
            plate_appearance_data['bases'] = bases
            if runner_scored:
                plate_appearance_data['runs'] += 1
                logger.log('\tRUN SCORED')
        return plate_appearance_data

    def adjust_count():
        if pitch_data['outcome'] == 'ball':
            plate_appearance.increment_balls()
        else:
            plate_appearance.increment_strikes()
        return plate_appearance_data

    def handle_auxiliary_event(bases):
        if pitch_data['wild_pitch']:  # if the pitch was a wild pitch, move the runners up (if applicable).
            bases, runner_scored = move_runners_up_a_base(bases)
            plate_appearance_data['bases'] = bases
            if runner_scored:
                plate_appearance_data['runs'] += 1
        return plate_appearance_data

    batter = lineup[place]
    if not batter.get_batting_stats():
        batter.set_batting_stats(batting_info.get(batter.get_player_id()), batting_year_info)
    if not pitcher.get_pitching_stats():
        pitcher.set_pitching_stats(pitching_info[pitcher.get_player_id()])
    plate_appearance_data = {'increment_batter': True, 'bases': bases,
                             'runs': 0}  # this will be the data passed back to the "inning".
    plate_appearance = PlateAppearance(batter, pitcher)  # create a PA object to track balls, strikes and outcome.
    batter_orientation = determine_batter_handedness_for_this_pa(batter, pitcher)
    pitcher_orientation = determine_pitcher_handedness_for_this_pa(pitcher)
    while plate_appearance.get_outcome() is None:  # while the plate appearance has not ended, simulate a pitch
        pitch_data = simulate_pitch(pitcher, batter, batter_orientation, pitcher_orientation,
                                    plate_appearance.get_balls(), plate_appearance.get_strikes(), strike_zone,
                                    pitching_year_info, logger)
        if pitch_data['pa_completed']:
            plate_appearance_data = wrap_up_plate_appearance(bases)  # plate appearance has concluded, so
        else:
            plate_appearance_data = adjust_count()  # plate appearance has not ended, so simply adjust the count.
        plate_appearance_data = handle_auxiliary_event(bases)  # handle any auxiliary events (e.g. SB, CS, WP, etc.)
################### random data ###################
    plate_appearance_data['hits'] = random.randint(0, 1)
    plate_appearance_data['errors'] = random.randint(0, 1)
################### random data ###################
    plate_appearance_data['lineup'] = lineup
    plate_appearance_data['pitcher'] = pitcher
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


def determine_batter_handedness_for_this_pa(batter, pitcher):
    """
    The batter could be a switch hitter. Determine what side of the plate the hitter will hit from depending on what
    hand the pitcher is throwing with for this particular plate appearance.
    :param batter: Batter(Player)
    :param pitcher: Pitcher(Player)
    :return: str -> 'vr' if RHB, otherwise 'vl'
    """
    if not batter.get_batting_handedness():
        batter.set_batting_handedness()
    if not pitcher.get_throwing_handedness():
        pitcher.retrieve_throwing_handedness()
    batter_orientation = batter.get_batting_handedness()
    if batter_orientation == 'B':
        batter_orientation = 'L' if pitcher.get_throwing_handedness() == 'R' else 'R'
    return 'v' + batter_orientation.lower()


def determine_pitcher_handedness_for_this_pa(pitcher):
    """
    The pitcher could throw with both hands (although very unlikely). Determine what hand the pitcher is throwing with
    for this particular plate appearance.
    :param pitcher: Pitcher(Player)
    :return: str -> 'vr' if RHP, otherwise vl.
    """
    if not pitcher.get_throwing_handedness():
        pitcher.retrieve_throwing_handedness()
    pitcher_orientation = pitcher.get_throwing_handedness()
    return 'v' + pitcher_orientation.lower()


def move_runners_up_a_base(bases, new_runner=None):
    if bases[3]:
        runner_scored = True
    else:
        runner_scored = False
    bases[3] = bases[2]
    bases[2] = bases[1]
    bases[1] = new_runner
    return bases, runner_scored

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
