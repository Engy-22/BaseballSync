import os
from model.pitch import Pitch
from utilities.logger import Logger
from utilities.properties import log_prefix
from controller.gauntlet import pick_from_options, pick_one_or_the_other

logger = Logger(os.path.join(log_prefix, "controller", "pitch.log"))


def simulate_pitch(pitcher, batter, batter_orientation, pitcher_orientation, balls, strikes, driver_logger):
    count = str(balls) + '-' + str(strikes)
    driver_logger.log('\tSimulating ' + count + ' pitch')
    logger.log('Simulating ' + count + ' pitch')
    pitch = Pitch(pitcher, determine_pitch_type(pitcher, batter, pitcher_orientation, batter_orientation, count),
                  balls, strikes)
    ball_strike = determine_ball_or_strike(pitcher, batter, pitcher_orientation, batter_orientation, count, pitch)
    batter_swing_rate = get_batter_swing_rate(batter, pitcher_orientation, count, ball_strike, pitch)
    swing_take = determine_if_batter_swung(batter_swing_rate, batter_orientation, pitcher, count, ball_strike, pitch)
    driver_logger.log('\t' + pitch.get_pitch_type() + ' - ' + ball_strike + ' - ' + swing_take)
    pitch_data = {'ball_strike': ball_strike, 'swing_take': swing_take,
                  'trajectory': '', 'field': '', 'direction': '', 'outcome': ''}
    return pitch_data


def determine_pitch_type(pitcher, batter, pitcher_orientation, batter_orientation, count):
    """pick a pitch to be thrown based on pitcher tendencies in this scenario and the types of pitches thrown to the
    batter in this scenario"""
    pitch_type = pick_from_options(coordinate_pitch_usages(
        pitcher.get_pitching_stats()['advanced_pitching_stats']['pitch_usage_pitching'][batter_orientation][count],
        batter, pitcher_orientation, count))
    if pitch_type:  # if the pitcher has been in this scenario before
        return pitch_type
    else:  # if the pitcher has not been in this scenario before, get their overall pitch usage numbers
        try:
            return pick_from_options(pitcher.get_pitching_stats()['advanced_pitching_stats']
                                     ['overall_pitch_usage_pitching'])
        except KeyError:
            print(pitcher.get_player_id())
            raise KeyError


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
    """ a batter may not have faced a pitcher with the same repertoire as this one, so take the pitches
    (and their frequencies) that the batter has seen and 'inflate' their values accordingly so their sum is equal to
    1.0 (or thereabout) """
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


def determine_ball_or_strike(pitcher, batter, pitcher_orientation, batter_orientation, count, pitch):
    """average the pitcher's strike percent and the batter's strike percent against in this scenario
    (assuming they're both present)"""
    try:
        return pick_one_or_the_other(
            (pitcher.get_pitching_stats()['advanced_pitching_stats']['strike_percent_pitching'][batter_orientation]
             [count][pitch.get_pitch_type()] + batter.get_batting_stats()['advanced_batting_stats']
             ['strike_percent_batting'][pitcher_orientation][count][pitch.get_pitch_type()]) / 2,
            {True: 'strike', False: 'ball'})
    except KeyError:  # there's no strike percent batter data for this scenario
        try:  # so just take the pitcher's strike percent data (assuming the pitcher's been in this scenario before)
            return pick_one_or_the_other(
                pitcher.get_pitching_stats()['advanced_pitching_stats']['strike_percent_pitching'][batter_orientation]
                [count][pitch.get_pitch_type()], {True: 'strike', False: 'ball'})
        except KeyError:  # if there's no strike percent data for the pitcher either, get the pitcher's overall strike percent data
            return pick_one_or_the_other(pitcher.get_pitching_stats()['advanced_pitching_stats']
                                         ['overall_strike_percent_pitching'][pitch.get_pitch_type()],
                                         {True: 'strike', False: 'ball'})


def get_batter_swing_rate(batter, pitcher_orientation, count, ball_strike, pitch):
    try:  # determine how often the batter swings given the scenario (assuming the batter has been in the scenario before)
        return batter.get_batting_stats()['advanced_batting_stats']['swing_rate_batting'][pitcher_orientation][count]\
            [ball_strike][pitch.get_pitch_type()]
    except KeyError:  # this batter has never been in this scenario before
        return None


def determine_if_batter_swung(batter_swing_rate, batter_orientation, pitcher, count, ball_strike, pitch):
    if batter_swing_rate:  # if the batter has data about this scenario
        try:  # average the batter's swing rate and the pitcher's swing rate against (assuming they're both present)
            return pick_one_or_the_other(
                (batter_swing_rate + pitcher.get_pitching_stats()['advanced_pitching_stats']['swing_rate_pitching']
                 [batter_orientation][count][ball_strike][pitch.get_pitch_type()]) / 2, {True: 'swing', False: 'take'})
        except KeyError:  # pitcher doesn't have data about this position, so just take the hitter's swing rate
            return pick_one_or_the_other(batter_swing_rate, {True: 'swing', False: 'take'})
    else:  # if the batter has no data about this scenario
        try:  # just take the pitcher's swing rate against him (assuming that data exists)
            return pick_one_or_the_other(
                pitcher.get_pitching_stats()['advanced_pitching_stats']['swing_rate_pitching'][batter_orientation]
                [count][ball_strike][pitch.get_pitch_type()], {True: 'swing', False: 'take'})
        except KeyError:  # there's no pitcher data in this scenario. Take the league average swing rate for the year of the batter
            return pick_one_or_the_other(0.5, {True: 'swing', False: 'take'})
