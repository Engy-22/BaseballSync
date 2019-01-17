"""
    Run this with the year(s) you want to download data for
    This populates all tables in the baseballData database
"""

import time
import datetime
from utilities.stringify_list import stringify_list
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
from import_data.player_data.pitch_fx import pitch_fx
from import_data.player_data.batting.batters import batting_constructor
from import_data.player_data.pitching.pitchers import pitching_constructor
from import_data.player_data.fielding.fielders import fielding_constructor
from import_data.team_data.team_fielding_file_constructor import team_fielding_file_constructor
from import_data.team_data.team_batting_order_constructor import team_batting_order_constructor
from import_data.team_data.team_pitching_rotation_constructor import team_pitching_rotation_constructor
from import_data.player_data.fielding.primary_and_secondary_positions import primary_and_secondary_positions
from import_data.player_data.batting.hitter_tendencies import hitter_tendencies
from import_data.player_data.pitching.pitcher_tendencies import pitcher_tendencies
from import_data.player_data.batting.hitter_spray_chart_constructor import hitter_spray_chart_constructor
from import_data.player_data.pitching.pitching_spray_chart_constructor import pitcher_spray_chart_constructor
from import_data.team_data.team_certainties import team_certainties
from import_data.team_data.rank_driver import rank_driver
from import_data.player_data.awards.award_winner_driver import award_winner_driver
from import_data.comparisions.comparisons_driver import comparisons_driver
from import_data.player_data.awards.hof_finder import hof_finder


def driver(year, driver_log):
    driver_log.log(str(year))
    driver_time = time.time()
    print('\n\n' + str(year))
    populate_teams_table(year, driver_log)
    get_year_data(year, driver_log)
    ballpark_and_manager_data(year, driver_log)
    league_standings(year, driver_log)
    team_offensive_statistics(year, driver_log)
    team_defensive_statistics(year, driver_log)
    # get_pitch_fx_data(year, driver_log)
    batting_constructor(year, driver_log)
    pitching_constructor(year, driver_log)
    fielding_constructor(year, driver_log)
    team_fielding_file_constructor(year, driver_log)
    team_batting_order_constructor(year, driver_log)
    team_pitching_rotation_constructor(year, driver_log)
    primary_and_secondary_positions(year, driver_log)
    hitter_tendencies(year, driver_log)
    pitcher_tendencies(year, driver_log)
    # hitter_spray_chart_constructor(year, driver_log)
    # pitcher_spray_chart_constructor(year, driver_log)
    team_certainties(year, driver_log)
    rank_driver(year, driver_log)
    award_winner_driver(year, driver_log)
    driver_log.log('Time taken to download ' + str(year) + ' data: ' + time_converter(time.time() - driver_time) + '\n')


if __name__ == '__main__':
    main_logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\driver.log")
    main_logger.log('Begin Driver || Timestamp: ' + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
    start_time = time.time()
    most_recent_year = get_most_recent_year(main_logger)
    league_table_constructor(main_logger)
    manager_table_constructor(main_logger)
    years = []
    for year in range(1876, 1999, 1):
        years.append(year)
        driver(year, main_logger)
    # if most_recent_year > 1997:
    #     comparisons_driver(most_recent_year)
    hof_finder(main_logger)
    main_logger.log('Driver complete for year' + stringify_list(years) + ': time = '
                    + time_converter(time.time() - start_time) + '\n\n\n')
