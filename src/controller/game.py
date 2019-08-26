import os
import time

from model.game import Game
from model.league import League
from model.teams.lineup_creator.get_starting_pitcher import get_starting_pitcher
from controller.inning import simulate_inning
from utilities.logger import Logger
from utilities.time_converter import time_converter
from utilities.properties import log_prefix, controller_driver_logger as driver_logger

logger = Logger(os.path.join(log_prefix, "controller", "game.log"))


def simulate_game(game_num, away_team, away_team_info, away_year_info, home_team, home_team_info, home_year_info,
                  away_year, home_year, strike_zone):
    driver_logger.log("Starting game " + str(game_num) + " simulation: " + away_team.get_team_id() + " @ "
                      + home_team.get_team_id())
    start_time = time.time()
    logger.log("Starting game " + str(game_num) + " simulation: " + away_team.get_team_id() + " @ "
               + home_team.get_team_id())
    game_data = {}
    game = Game(away_team.get_team_id(), home_team.get_team_id())
    league = League(home_team.get_team_id(), home_team.get_year())
    lineup_time = time.time()
    logger.log("\tCreating lineups")
    away_pitcher = get_starting_pitcher(away_team.get_team_id(), away_year, game_num)
    home_pitcher = get_starting_pitcher(home_team.get_team_id(), home_year, game_num)
    away_team.set_lineup(away_pitcher, home_pitcher, use_dh=league.get_rules())
    home_team.set_lineup(home_pitcher, away_pitcher, use_dh=league.get_rules())
    logger.log("\t\t" + time_converter(time.time() - lineup_time))
    while game.get_inning() <= 9 or game.get_away_score() == game.get_home_score():
        inning_data = simulate_inning(game, away_team_info, home_team_info, away_year_info, home_year_info,
                                      {'top': away_team.get_batting_order(), 'bottom': home_team.get_batting_order()},
                                      {'top': away_team.get_lineup_place(), 'bottom': home_team.get_lineup_place()},
                                      {'top': home_team.get_pitcher(), 'bottom': away_team.get_pitcher()}, strike_zone,
                                      logger)
        away_team.set_batting_order(inning_data['top']['lineup'])
        home_team.set_batting_order(inning_data['bottom']['lineup'])
        away_team.set_lineup_place(inning_data['top']['place'])
        home_team.set_lineup_place(inning_data['bottom']['place'])
        game.increment_away_score(inning_data['top']['runs'])
        game.increment_home_score(inning_data['bottom']['runs'])
    game_data['winner'] = determine_winner(game, away_team, home_team)
    total_time = time_converter(time.time() - start_time)
    logger.log("Done simulating game: " + away_team.get_team_id() + " @ " + home_team.get_team_id() + ": Time = "
               + total_time + '\n\n')
    driver_logger.log("\tDone simulating game: " + away_team.get_team_id() + " @ " + home_team.get_team_id()
                      + ": Time = " + total_time)
    return game_data


def determine_winner(game, away_team, home_team):
    if game.get_away_score() > game.get_home_score():
        logger.log(away_team.get_team_id() + ' won: ' + str(game.get_away_score()) + ' to '
                   + str(game.get_home_score()))
        return away_team.get_team_id()
    else:
        logger.log(home_team.get_team_id() + ' won: ' + str(game.get_home_score()) + ' to '
                   + str(game.get_away_score()))
        return home_team.get_team_id()
