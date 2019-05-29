import os
import time
from utilities.logger import Logger
from model.game import Game
from model.teams.team import Team
from model.league import League
from controller.inning import simulate_inning
from utilities.time_converter import time_converter
from utilities.properties import controller_driver_logger as driver_logger

logger = Logger(os.path.join("..", "..", "logs", "sandbox", "controller", "game.log"))


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
    away_team.set_lineup(game_num, use_dh=league.get_rules())
    home_team.set_lineup(game_num, use_dh=league.get_rules())
    logger.log("\t\t" + time_converter(time.time() - lineup_time))
    while game.get_inning() <= 9 or game.get_away_score() == game.get_home_score():
        inning_data = simulate_inning(game, {'top': away_team.get_batting_order(),
                                             'bottom': home_team.get_batting_order()},
                                      {'top': away_team.get_lineup_place(), 'bottom': home_team.get_lineup_place()},
                                      {'top': home_team.get_pitcher(), 'bottom': away_team.get_pitcher()}, logger)
        away_team.set_batting_order(inning_data['top']['lineup'])
        home_team.set_batting_order(inning_data['bottom']['lineup'])
        away_team.set_lineup_place(inning_data['top']['place'])
        home_team.set_lineup_place(inning_data['bottom']['place'])
        game.increment_away_score(inning_data['top']['runs'])
        game.increment_home_score(inning_data['bottom']['runs'])
    total_time = time_converter(time.time() - start_time)
    logger.log("Done simulating game: " + away_team_id + " @ " + home_team_id + ": Time = " + total_time + '\n\n')
    driver_logger.log("Done simulating game: " + away_team_id + " @ " + home_team_id + ": Time = " + total_time)
    return game_data
