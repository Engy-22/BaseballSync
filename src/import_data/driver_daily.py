"""
    Run this with the day you want to download data for.
    This populates all tables in the baseballData database.
"""

import time
import datetime
from utilities.time_converter import time_converter
from import_data.leagues.league_table_constructor import league_table_constructor
from import_data.team_data.manager_table_constructor import manager_table_constructor
from import_data.team_data.populate_teams_table import populate_teams_table
from import_data.leagues.year_data import get_year_data
from import_data.team_data.ballpark_and_manager_data import ballpark_and_manager_data
from import_data.leagues.league_standings import league_standings
from import_data.team_data.team_offensive_statistics import team_offensive_statistics
from import_data.team_data.team_defensive_statistics import team_defensive_statistics
from import_data.player_data.pitch_fx.pitch_fx import get_pitch_fx_data
from import_data.player_data.batting.batters import batting_constructor
from import_data.player_data.pitching.pitchers import pitching_constructor
from import_data.player_data.fielding.fielders import fielding_constructor
from import_data.team_data.team_fielding_file_constructor import team_fielding_file_constructor
from import_data.team_data.team_batting_order_constructor import team_batting_order_constructor
from import_data.team_data.team_pitching_rotation_constructor import team_pitching_rotation_constructor
from import_data.player_data.fielding.primary_and_secondary_positions import primary_and_secondary_positions
from import_data.player_data.batting.hitter_tendencies import hitter_tendencies
from import_data.player_data.pitching.pitcher_tendencies import pitcher_tendencies
from import_data.team_data.manager_tendencies import manager_tendencies
from import_data.player_data.batting.hitter_spray_chart_constructor import hitter_spray_chart_constructor
from import_data.player_data.pitching.pitching_spray_chart_constructor import pitcher_spray_chart_constructor
from import_data.team_data.team_certainties import team_certainties
from utilities.clean_up_deadlocked_file import clean_up_deadlocked_file
from import_data.consolidata.driver import consolidate_data
from import_data.player_data.pitching.determine_pitcher_roles import determine_pitcher_roles_year
from utilities.properties import import_driver_logger as driver_logger
from utilities.database.automated_migration import auto_migrate
from import_data.leagues.create_strike_zone import create_strike_zone
from import_data.email_results import send_results


def main(from_server, day, month, year, frame=None):
    print('\n')
    if 0 < day <= 31 and 0 < month <= 12 and year >= 1876:
        try:
            driver_logger.log('Begin Daily Driver || Timestamp: ' + datetime.datetime.today().
                              strftime('%Y-%m-%d %H:%M:%S'))
            start_time = time.time()
            if not from_server:
                frame.withdraw()
            league_table_constructor()
            manager_table_constructor()
            driver(day, month, year)
            create_strike_zone()
            clean_up_deadlocked_file()
            auto_migrate()
            driver_logger.log('Driver complete for year ' + str(year) + ': time = '
                              + time_converter(time.time()-start_time) + '\n')
        except Exception as e:
            driver_logger.log("ERROR:\t" + str(e))
            send_results()
            raise e
    else:
        print('Must enter a valid date.')
    send_results()
    exit()


def driver(day, month, year):
    driver_logger.log(str(month) + '/' + str(day) + '/' + str(year))
    driver_time = time.time()
    print('\n\n' + str(month) + '/' + str(day) + '/' + str(year))
    populate_teams_table(year)
    get_year_data(year)
    ballpark_and_manager_data(year)
    league_standings(year)
    team_offensive_statistics(year)
    team_defensive_statistics(year)
    batting_constructor(year)
    pitching_constructor(year)
    fielding_constructor(year)
    team_fielding_file_constructor(year)
    team_pitching_rotation_constructor(year)
    team_batting_order_constructor(year)
    primary_and_secondary_positions(year)
    determine_pitcher_roles_year(year)
    get_pitch_fx_data(year, month, day)
    hitter_tendencies(year)
    pitcher_tendencies(year)
    manager_tendencies(year)
    hitter_spray_chart_constructor(year)
    pitcher_spray_chart_constructor(year)
    team_certainties(year)
    consolidate_data(year)
    driver_logger.log('Time taken to download ' + str(month) + '/' + str(day) + '/' + str(year) + ' data: '
                      + time_converter(time.time()-driver_time) + '\n')
