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


def driver(year):
    print('\n\n' + str(year))
    populate_teams_table(year)
    get_year_data(year)
    ballpark_and_manager_data(year)


if __name__ == '__main__':
    main_logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\driver.log")
    main_logger.log('Begin Driver || Timestamp: ' + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
    start_time = time.time()
    try:
        most_recent_year = get_most_recent_year()
    except IndexError:
        most_recent_year = 1876
    league_table_constructor()
    manager_table_constructor()
    driver(2018)
    # if most_recent_year > 1997:
    #     comparisons_driver()
    # hof_finder()
    main_logger.log('\nDriver complete: time = ' + time_converter(time.time() - start_time))
