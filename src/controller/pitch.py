import random
import os
from utilities.logger import Logger
from model.pitch import Pitch

logger = Logger(os.path.join("..", "logs", "sandbox", "controller", "pitch.log"))


def simulate_pitch(pitcher, batter, balls, strikes, inning, driver_logger):
    driver_logger.log('Simulating ' + str(balls) + '-' + str(strikes) + ' pitch')
    logger.log('Simulating ' + str(balls) + '-' + str(strikes) + ' pitch')
    pitch = Pitch(pitcher, balls, strikes)
    if pitcher.get_pitching_stats() is None:
        pitcher.retrieve_pitching_stats()
    pitch_data = {}
    pitch_data['ball_strike'] = ball_strike()
    pitch_data['trajectory'] = ''
    pitch_data['field'] = ''
    pitch_data['direction'] = ''
    pitch_data['outcome'] = ''
    return pitch_data


################### random data ###################
def ball_strike():
    if random.randint(0, 1) == 0:
        return 'ball'
    else:
        return 'strike'
################### random data ###################
