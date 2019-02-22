from utilities.logger import Logger
from model.game import Game
from model.teams.team import Team
from controller.inning import simulate_inning
import time
from utilities.time_converter import time_converter

logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\controller\\game.log")


def simulate_game(game_num, away_team_id, away_year, home_team_id, home_year, driver_logger):
    driver_logger.log("\tStarting game simulation: " + away_team_id + " @ " + home_team_id)
    start_time = time.time()
    logger.log("Starting game simulation: " + away_team_id + " @ " + home_team_id)
    game_data = {}
    game = Game(away_team_id, home_team_id)
    away_team = Team(away_team_id, away_year)
    away_lineup = away_team.create_lineup(game_num)
    home_team = Team(home_team_id, home_year)
    while game.get_inning() <= 9 or game.away_score == game.home_score:
        inning_data = simulate_inning(game, logger)
    total_time = time_converter(time.time() - start_time)
    logger.log("Done simulating game: " + away_team_id + " @ " + home_team_id + ": Time = " + total_time + '\n\n')
    driver_logger.log("Done simulating game: " + away_team_id + " @ " + home_team_id + ": Time = " + total_time)
    return game_data
