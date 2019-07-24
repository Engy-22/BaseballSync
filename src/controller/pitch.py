import os
from model.pitch import Pitch
from utilities.logger import Logger
from utilities.properties import log_prefix
from controller.gauntlet import pick_from_options, pick_one_or_the_other

logger = Logger(os.path.join(log_prefix, "controller", "pitch.log"))


def simulate_pitch(pitcher, batter, batter_orientation, pitcher_orientation, balls, strikes, driver_logger):
    count = str(balls) + '-' + str(strikes)
    driver_logger.log('Simulating ' + count + ' pitch')
    logger.log('Simulating ' + count + ' pitch')
    pitch = Pitch(pitcher, pick_from_options(pitcher.get_pitching_stats()['advanced_pitching_stats']
                                             ['pitch_usage_pitching'][batter_orientation][count]), balls, strikes)
    ball_strike = pick_one_or_the_other(pitcher.get_pitching_stats()['advanced_pitching_stats']
                                        ['strike_percent_pitching'][batter_orientation][count][pitch.get_pitch_type()],
                                        {True: 'strike', False: 'ball'})
    try:
        swing_take = pick_one_or_the_other(batter.get_batting_stats()['advanced_batting_stats']['swing_rate_batting']
                                           [pitcher_orientation][count][pitch.get_pitch_type()],
                                           {True: 'swing', False: 'take'})
    except KeyError:
        swing_take = None
    # driver_logger.log('\t' + pitch.get_pitch_type() + ' - ' + ball_strike + ' - ' + swing_take)
    pitch_data = {'ball_strike': ball_strike, 'swing_take': swing_take,
                  'trajectory': '', 'field': '', 'direction': '', 'outcome': ''}
    return pitch_data
