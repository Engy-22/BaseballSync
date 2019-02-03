import random
from utilities.logger import Logger

logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\controller\\pitch.log")


def simulate_pitch(balls, strikes, driver_logger):
    driver_logger.log('Simulating ' + str(balls) + '-' + str(strikes) + ' pitch')
    logger.log('Simulating ' + str(balls) + '-' + str(strikes) + ' pitch')
    pitch_data = {}
    pitch_data['pitch_type'] = ''
    pitch_data['ball_strike'] = ''
    pitch_data['trajectory'] = ''
    pitch_data['field'] = ''
    pitch_data['direction'] = ''
    pitch_data['outcome'] = ''
    return pitch_data
