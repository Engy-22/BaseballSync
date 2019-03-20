import os
from utilities.logger import Logger
from model.game import Game
from model.teams.team import Team
from model.league import League
from controller.inning import simulate_inning
import time
from utilities.time_converter import time_converter
from utilities.properties import controller_driver_logger as driver_logger

logger = Logger(os.path.join("..", "..", "logs", "controller", "game.log"))


def simulate_game(game_num, away_team_id, away_year, home_team_id, home_year):
    driver_logger.log("\tStarting game simulation: " + away_team_id + " @ " + home_team_id)
    start_time = time.time()
    logger.log("Starting game simulation: " + away_team_id + " @ " + home_team_id)
    game_data = {}
    game = Game(away_team_id, home_team_id)
    team_object_time = time.time()
    logger.log("\tCreating team objects")
    away_team = Team(away_team_id, away_year)
    home_team = Team(home_team_id, home_year)
    logger.log("\t\t" + time_converter(time.time() - team_object_time))
    league = League(home_team.get_team_id(), home_team.get_year())
    lineup_time = time.time()
    logger.log("\tCreating lineups")
    away_lineup = away_team.set_lineup(game_num, use_dh=league.get_rules())
    home_lineup = home_team.set_lineup(game_num, use_dh=league.get_rules())
    logger.log("\t\t" + time_converter(time.time() - lineup_time))
    # while game.get_inning() <= 9 or game.away_score == game.home_score:
    #     inning_data = simulate_inning(game, logger)
    total_time = time_converter(time.time() - start_time)
    logger.log("Done simulating game: " + away_team_id + " @ " + home_team_id + ": Time = " + total_time + '\n\n')
    driver_logger.log("Done simulating game: " + away_team_id + " @ " + home_team_id + ": Time = " + total_time)
    return game_data


# for game in range(200):
#     simulate_game(game+1, 'DET', 2013, 'STL', 2013)
