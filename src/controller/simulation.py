import time
import datetime
from model.teams.team import Team
from utilities.time_converter import time_converter
from controller.game import simulate_game
from utilities.properties import controller_driver_logger as logger
from utilities.clear_logs import clear_logs

logger.log('Beginning simulation || Timestamp: ' + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S'))


def simulation(away_team_id, away_year, home_team_id, home_year, games):
    start_time = time.time()
    clear_logs('controller')
    team_object_time = time.time()
    logger.log("Creating team objects")
    away_team = Team(away_team_id, away_year)
    home_team = Team(home_team_id, home_year)
    logger.log("\t" + time_converter(time.time() - team_object_time))
    away_team_wins = 0
    home_team_wins = 0
    for game in range(games):
        game_data = simulate_game(game+1, away_team, away_team.get_team_info(), home_team, home_team.get_team_info(),
                                  away_year, home_year)
        if game_data['winner'] == away_team.get_team_id():
            away_team_wins += 1
        else:
            home_team_wins += 1
    logger.log('Simulation complete: Time = ' + time_converter(time.time() - start_time))
    return determine_series_winner(away_team, away_team_wins, home_team, home_team_wins, games)


def determine_series_winner(away_team, away_wins, home_team, home_wins, games):
    if away_wins != home_wins:
        if away_wins > home_wins:
            winner = away_team
            logger.log('\t' + winner.get_team_id() + ' wins the ' + str(games) + ' game series - ' + str(away_wins)
                       + ' to ' + str(home_wins))
        else:
            winner = home_team
            logger.log('\t' + winner.get_team_id() + ' wins the ' + str(games) + ' game series - ' + str(home_wins)
                       + ' to ' + str(away_wins))
        return winner.get_team_id() + ' wins the ' + str(games) + ' game series: ' + away_team.get_team_id()\
               + ' ' + str(away_wins) + ' | ' + home_team.get_team_id() + ' ' + str(home_wins)
    else:
        return 'The ' + str(away_wins+home_wins) + ' game series has ended in a tie'


simulation('MIN', 2017, 'CLE', 2017, 1)
