from utilities.logger import Logger
from model.Game import Game
from controller.inning import simulate_inning
import time
from utilities.time_converter import time_converter

logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\controller\\game.log")


def simulate_game(away_team, home_team, driver_logger):
    driver_logger.log("\tStarting game simulation: " + away_team + " @ " + home_team)
    start_time = time.time()
    logger.log("Starting game simulation: " + away_team + " @ " + home_team)
    game_data = {}
    game = Game(away_team, home_team)
    while game.get_inning() <= 9 or game.away_score == game.home_score:
        inning_data = simulate_inning(game, logger)
    total_time = time_converter(time.time() - start_time)
    logger.log("Done simulating game: " + away_team + " @ " + home_team + ": Time = " + total_time + '\n\n')
    driver_logger.log("Done simulating game: " + away_team + " @ " + home_team + ": Time = " + total_time)
    return game_data
