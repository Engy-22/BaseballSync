"""
    Run this with the year(s) you want to download data for
    This populates all tables in the baseballData database
"""

import threading
from import_data.team_data.populate_teams_table import populate_teams_table


def driver(year):
    print('\n\n' + str(year))
    populate_teams_table(year)


driver(2018)
