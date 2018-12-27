"""
    Run this with the year(s) you want to download data for
    This populates all tables in the baseballData database
"""

import time
import datetime
from utilities.Logger import Logger
from utilities.time_converter import time_converter
from utilities.get_most_recent_year import get_most_recent_year
from import_data.leagues.league_table_constructor import league_table_constructor
from import_data.team_data.manager_table_constructor import manager_table_constructor
from import_data.team_data.populate_teams_table import populate_teams_table
from import_data.leagues.year_data import get_year_data
from import_data.team_data.ballpark_and_manager_data import ballpark_and_manager_data
from import_data.leagues.league_standings import league_standings
from import_data.team_data.team_offensive_statistics import team_offensive_statistics
from import_data.team_data.team_defensive_statistics import team_defensive_statistics
from import_data.player_data.pitch_fx import get_pitch_fx_data


def driver(year, driver_log):
    print('\n\n' + str(year))
    populate_teams_table(year, driver_log)
    get_year_data(year, driver_log)
    ballpark_and_manager_data(year, driver_log)
    league_standings(year, driver_log)
    team_offensive_statistics(year, driver_log)
    team_defensive_statistics(year, driver_log)
    # get_pitch_fx_data(year, driver_log)


if __name__ == '__main__':
    main_logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\driver.log")
    main_logger.log('Begin Driver || Timestamp: ' + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
    start_time = time.time()
    try:
        most_recent_year = get_most_recent_year(main_logger)
    except IndexError:
        most_recent_year = 1876
    league_table_constructor(main_logger)
    manager_table_constructor(main_logger)
    driver(2018, main_logger)
    # if most_recent_year > 1997:
    #     comparisons_driver()
    # hof_finder()
    main_logger.log('Driver complete: time = ' + time_converter(time.time() - start_time) + '\n\n')
