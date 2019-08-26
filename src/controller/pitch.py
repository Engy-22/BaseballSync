import os

from model.pitch import Pitch
from utilities.logger import Logger
from utilities.properties import log_prefix
from controller.gauntlet import pick_from_options, pick_one_or_the_other, get_location

logger = Logger(os.path.join(log_prefix, "controller", "pitch.log"))


def simulate_pitch(pitcher, batter, batter_orientation, pitcher_orientation, balls, strikes, strike_zone,
                   pitching_year_info, driver_logger):
    count = str(balls) + '-' + str(strikes)
    driver_logger.log('\tSimulating ' + count + ' pitch')
    logger.log('Simulating ' + count + ' pitch')
    pitch = Pitch(pitcher, determine_pitch_type(pitcher, batter, pitcher_orientation, batter_orientation, count),
                  balls, strikes)
    pitch_location = determine_pitch_location(pitcher, batter, pitcher_orientation, batter_orientation, count, pitch)
    ball_strike = determine_ball_strike(pitch_location, strike_zone)
    swing_take = determine_if_batter_swung(
        pitching_year_info, get_batter_swing_rate(batter, pitcher_orientation, count, ball_strike, pitch),
        batter_orientation, pitcher,
        batter, count, ball_strike, pitch)
    outcome = determine_outcome(balls, strikes, ball_strike, swing_take)
    driver_logger.log('\t\t' + pitch.get_pitch_type() + ' - ' + ball_strike + ' - ' + swing_take)
    return {'swing_take': swing_take, 'trajectory': '', 'field': '', 'direction': '', 'outcome': outcome,
            'pa_completed': pa_completed(outcome), 'wild_pitch': False}


def determine_pitch_type(pitcher, batter, pitcher_orientation, batter_orientation, count):
    """
    pick a pitch to be thrown based on pitcher tendencies in this scenario and the types of pitches thrown to the
    batter in this scenario
    """
    pitch_type = pick_from_options(coordinate_pitch_usages(
        pitcher.get_pitching_stats()['advanced_pitching_stats']['pitch_usage_pitching'][batter_orientation][count],
        batter, pitcher_orientation, count))
    if pitch_type:  # if the pitcher has been in this scenario before
        return pitch_type
    else:  # if the pitcher has not been in this scenario before, get their overall pitch usage numbers
        return pick_from_options(pitcher.get_pitching_stats()['advanced_pitching_stats']
                                 ['overall_pitch_usage_pitching'])


def coordinate_pitch_usages(pitches_available, batter, pitcher_orientation, count):
    """ get the frequency with which pitches are thrown in this particular count """
    pitches_available_for_this_scenario = {}
    batter_pitches_seen = inflate_pitch_options(batter.get_batting_stats()['advanced_batting_stats']
                                                ['pitch_usage_batting'][pitcher_orientation][count], pitches_available)  # get the pitches (and frequency of them) that the batter has seen in this scenario
    for pitch, frequency in pitches_available.items():  # loop through the pitches that the pitcher throws in this scenario
        try:
            pitches_available_for_this_scenario[pitch] = (frequency + batter_pitches_seen[pitch]) / 2  # average the frequency that the pitcher throws this pitch in this scenario and the frequency that the batter sees this pitch in this scenario
        except KeyError:  # if the batter never saw this pitch in this scenario in this year, simply take the pitcher's frequency of throwing this pitch in this scenario
            pitches_available_for_this_scenario[pitch] = frequency
    return pitches_available_for_this_scenario


def inflate_pitch_options(batter_pitches, pitcher_pitches):
    """
     a batter may not have faced a pitcher with the same repertoire as this one, so take the pitches
    (and their frequencies) that the batter has seen and 'inflate' their values accordingly so their sum is equal to
    1.0 (or thereabout)
    """
    temp_pitch_collection = {}
    pitch_collection = {}
    total = 0
    for pitch in pitcher_pitches.keys():
        try:
            temp_pitch_collection[pitch] = batter_pitches[pitch]
            total += batter_pitches[pitch]
        except KeyError:
            continue
    for pitch, frequency in temp_pitch_collection.items():
        pitch_collection[pitch] = frequency / total
    return pitch_collection


def determine_pitch_location(pitcher, batter, pitcher_orientation, batter_orientation, count, pitch):
    """
    average out the batter and pitcher means for the pitch thrown in the given count, as well as their standard
    deviations (assuming this information is available given the scenario) and determine an x & y coordinate based upon
    these values. Then use strike_zone.json to determine whether the pitch was a strike or not.
    """
    try:  # assuming pitcher and batter data is present
        average_x_mean = \
            (pitcher.get_pitching_stats()['advanced_pitching_stats']['pitch_location_pitching'][batter_orientation]
             [count][pitch.get_pitch_type() + '_x_mean'] + batter.get_batting_stats()['advanced_batting_stats']
             ['pitch_location_batting'][pitcher_orientation][count][pitch.get_pitch_type() + '_x_mean']) / 2
        average_x_deviation = \
            (pitcher.get_pitching_stats()['advanced_pitching_stats']['pitch_location_pitching'][batter_orientation]
             [count][pitch.get_pitch_type() + '_x_stdev'] + batter.get_batting_stats()['advanced_batting_stats']
             ['pitch_location_batting'][pitcher_orientation][count][pitch.get_pitch_type() + '_x_stdev']) / 2
        average_y_mean = \
            (pitcher.get_pitching_stats()['advanced_pitching_stats']['pitch_location_pitching'][batter_orientation]
             [count][pitch.get_pitch_type() + '_y_mean'] + batter.get_batting_stats()['advanced_batting_stats']
             ['pitch_location_batting'][pitcher_orientation][count][pitch.get_pitch_type() + '_y_mean']) / 2
        average_y_deviation = \
            (pitcher.get_pitching_stats()['advanced_pitching_stats']['pitch_location_pitching'][batter_orientation]
             [count][pitch.get_pitch_type() + '_y_stdev'] + batter.get_batting_stats()['advanced_batting_stats']
             ['pitch_location_batting'][pitcher_orientation][count][pitch.get_pitch_type() + '_y_stdev']) / 2
    except KeyError:  # batter data is not present for this scenario
        try:  # just take the pitcher's mean and deviation for pitch locations for this scenario
            average_x_mean = pitcher.get_pitching_stats()['advanced_pitching_stats']['pitch_location_pitching']\
                [batter_orientation][count][pitch.get_pitch_type() + '_x_mean']
            average_x_deviation = pitcher.get_pitching_stats()['advanced_pitching_stats']['pitch_location_pitching']\
                [batter_orientation][count][pitch.get_pitch_type() + '_x_stdev']
            average_y_mean = pitcher.get_pitching_stats()['advanced_pitching_stats']['pitch_location_pitching']\
                [batter_orientation][count][pitch.get_pitch_type() + '_y_mean']
            average_y_deviation = pitcher.get_pitching_stats()['advanced_pitching_stats']['pitch_location_pitching']\
                [batter_orientation][count][pitch.get_pitch_type() + '_y_stdev']
        except KeyError:  # pitcher data is not preset either
            try:  # take batter and pitcher overall average mean and deviation for the pitch type, regardless of count
                average_x_mean = \
                    (pitcher.get_pitching_stats()['advanced_pitching_stats']['overall_pitch_location_pitching']
                     [pitch.get_pitch_type() + '_x_mean'] + batter.get_batting_stats()['advanced_batting_stats']
                     ['overall_pitch_location_batting'][pitch.get_pitch_type() + '_x_mean']) / 2
                average_x_deviation = \
                    (pitcher.get_pitching_stats()['advanced_pitching_stats']['overall_pitch_location_pitching']
                     [pitch.get_pitch_type() + '_x_stdev'] + batter.get_batting_stats()['advanced_batting_stats']
                     ['overall_pitch_location_batting'][pitch.get_pitch_type() + '_x_stdev']) / 2
                average_y_mean = \
                    (pitcher.get_pitching_stats()['advanced_pitching_stats']['overall_pitch_location_pitching']
                     [pitch.get_pitch_type() + '_y_mean'] + batter.get_batting_stats()['advanced_batting_stats']
                     ['overall_pitch_location_batting'][pitch.get_pitch_type() + '_y_mean']) / 2
                average_y_deviation = \
                    (pitcher.get_pitching_stats()['advanced_pitching_stats']['overall_pitch_location_pitching']
                     [pitch.get_pitch_type() + '_y_stdev'] + batter.get_batting_stats()['advanced_batting_stats']
                     ['overall_pitch_location_batting'][pitch.get_pitch_type() + '_y_stdev']) / 2
            except KeyError:  # overall batter data is not available for this pitch, just take the pitcher's overall mean and deviation for the pitch, regardless of count/match up
                average_x_mean = pitcher.get_pitching_stats()['advanced_pitching_stats']\
                    ['overall_pitch_location_pitching'][pitch.get_pitch_type() + '_x_mean']
                average_x_deviation = pitcher.get_pitching_stats()['advanced_pitching_stats']\
                    ['overall_pitch_location_pitching'][pitch.get_pitch_type() + '_x_stdev']
                average_y_mean = pitcher.get_pitching_stats()['advanced_pitching_stats']\
                    ['overall_pitch_location_pitching'][pitch.get_pitch_type() + '_y_mean']
                average_y_deviation = pitcher.get_pitching_stats()['advanced_pitching_stats']\
                    ['overall_pitch_location_pitching'][pitch.get_pitch_type() + '_y_stdev']
    return get_location(average_x_mean, average_x_deviation), get_location(average_y_mean, average_y_deviation)


def determine_ball_strike(pitch_location, strike_zone):
    if pitch_in_zone(pitch_location[0], pitch_location[1], strike_zone.get('x'), strike_zone.get('y')):
        return 'strike'
    else:
        return 'ball'


def pitch_in_zone(x, y, x_coordinates, y_coordinates):
    return x_coordinates['low'] < x < x_coordinates['high'] and y_coordinates['low'] < y < y_coordinates['high']


def get_batter_swing_rate(batter, pitcher_orientation, count, ball_strike, pitch):
    """determine how often the batter swings given the scenario (assuming the batter has been in the scenario before)"""
    try:
        return batter.get_batting_stats()['advanced_batting_stats']['swing_rate_batting'][pitcher_orientation][count]\
            [ball_strike][pitch.get_pitch_type()]
    except KeyError:  # this batter has never been in this scenario before
        return None


def determine_if_batter_swung(pitching_year_info, batter_swing_rate, batter_orientation, pitcher, batter, count,
                              ball_strike, pitch):
    """determine if the batter swung at the pitch or not based on his tendency to do so given the count, pitch_type,
    match up and location; in accordance with the pitcher's induced swing rate given the count pitch_type, match up and
    location"""
    if batter_swing_rate:  # if the batter has data about this scenario
        try:  # average the batter's swing rate and the pitcher's swing rate against (assuming they're both present)
            return pick_one_or_the_other(
                (batter_swing_rate + pitcher.get_pitching_stats()['advanced_pitching_stats']['swing_rate_pitching']
                 [batter_orientation][count][ball_strike][pitch.get_pitch_type()]) / 2, {True: 'swing', False: 'take'})
        except KeyError:  # pitcher doesn't have data about this position, so just take the hitter's swing rate
            return pick_one_or_the_other(batter_swing_rate, {True: 'swing', False: 'take'})
    else:  # if the batter has no data in this scenario
        try:  # just take the pitcher's swing rate against (assuming that data exists)
            return pick_one_or_the_other(
                pitcher.get_pitching_stats()['advanced_pitching_stats']['swing_rate_pitching'][batter_orientation]
                [count][ball_strike][pitch.get_pitch_type()], {True: 'swing', False: 'take'})
        except KeyError:  # there's also no pitcher data in this scenario.
            try:  # Take the overall swing rate average for the pitcher and hitter on this particular pitch (assuming the batter has seen this pitch before)
                return pick_one_or_the_other(
                    (pitcher.get_pitching_stats()['advanced_pitching_stats']['overall_swing_rate_pitching'][ball_strike]
                     [pitch.get_pitch_type()] + batter.get_batting_stats()['advanced_batting_stats']
                     ['overall_swing_rate_batting'][ball_strike][pitch.get_pitch_type()]) / 2,
                    {True: 'swing', False: 'take'})
            except KeyError:  # the batter hasn't seen this pitch before, so just take pitcher's overall swing rate against on this pitch
                try:
                    return pick_one_or_the_other(
                        pitcher.get_pitching_stats()['advanced_pitching_stats']['overall_swing_rate_pitching']
                        [ball_strike][pitch.get_pitch_type()], {True: 'swing', False: 'take'})
                except KeyError:
                    print('adsfasdfasdfasdfasdfasfadsfasdfasdf')
                    return pick_one_or_the_other(pitching_year_info['swing_rate_pitching'][ball_strike]
                                                 [pitch.get_pitch_type()], {True: 'swing', False: 'take'})


def determine_outcome(balls, strikes, ball_strike, swing_take):
    if ball_strike == 'strike':
        if strikes == 2:
            if swing_take == 'swing':
                return 'K (swinging)'
            else:
                return 'K (looking)'
        else:
            if swing_take == 'swing':
                return 'strike (swinging)'
            else:
                return 'strike (looking)'
    else:
        if balls == 3 and ball_strike == 'ball' and swing_take == 'take':
            return 'BB'
        elif swing_take == 'swing':
            if strikes == 2:
                return 'K (swinging)'
            else:
                return 'strike (swinging)'
        else:
            return 'ball'


def pa_completed(pitch_outcome):
    return pitch_outcome in ['BB', 'K (swinging)', 'K (looking)']
