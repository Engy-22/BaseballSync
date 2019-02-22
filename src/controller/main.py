import time
import datetime
from utilities.logger import Logger
from utilities.time_converter import time_converter
from controller.game import simulate_game

logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\controller\\main.log")


start_time = time.time()
logger.log('Beginning simulation || Timestamp: ' + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
if __name__ == '__main__':
    game_num = 1
    game_data = simulate_game(game_num, '', '', '', '', logger)
logger.log('Simulation complete: Time = ' + time_converter(time.time() - start_time))
