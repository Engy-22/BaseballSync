"""
    Run this with the year(s) you want to download data for
    This populates all tables in the baseballData database
"""

import threading
from import_data.team_data.populate_teams_table import populate_teams_table
from import_data.leagues.year_data import get_year_data


def driver(year):
    print('\n\n' + str(year))
    populate_teams_table(year)
    get_year_data(year)


if __name__ == '__main__':
    driver(2018)
