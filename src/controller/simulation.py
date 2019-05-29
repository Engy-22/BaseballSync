import time
import datetime
from model.teams.team import Team
from utilities.time_converter import time_converter
from controller.game import simulate_game
from utilities.properties import controller_driver_logger as logger
from utilities.clear_logs import clear_logs


start_time = time.time()
logger.log('Beginning simulation || Timestamp: ' + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
if __name__ == '__main__':
    clear_logs('controller')
    games = 6
    away_team_id = 'CLE'
    away_year = 2017
    home_team_id = 'PHI'
    home_year = 2017
    team_object_time = time.time()
    logger.log("Creating team objects")
    away_team = Team(away_team_id, away_year)
    home_team = Team(home_team_id, home_year)
    logger.log("\t" + time_converter(time.time() - team_object_time))
    for game in range(games):
        game_data = simulate_game(game+1, away_team, home_team)
logger.log('Simulation complete: Time = ' + time_converter(time.time() - start_time))
