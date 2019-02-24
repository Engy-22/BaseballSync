from utilities.connections.baseball_data_connection import DatabaseConnection
from utilities.properties import sandbox_mode


def get_hitter_stats(hitter, year):
    stats = {'R': 0, 'RBI': 0, 'H': 0, '2B': 0, '3B': 0, 'HR': 0, 'SB': 0, 'BB': 0, 'SO': 0}
    db = DatabaseConnection(sandbox_mode)
    pa = int(db.read('select player_batting.pa from player_batting, player_teams where player_batting.'
                     'PT_uniqueidentifier = player_teams.PT_uniqueidentifier and ' + 'player_teams.playerId = "'
                     + hitter + '" and year = ' + str(year) + ';')[0][0])
    for key, value in stats.items():
        try:
            stats[key] = int(db.read('select player_batting.' + key + ' from player_batting, player_teams where '
                                     'player_batting.PT_uniqueidentifier = player_teams.PT_uniqueidentifier and '
                                     'player_teams.playerId = "' + hitter + '" and year = ' + str(year) + ';')[0][0])
        except TypeError:
            continue
    db.close()
    return pa, stats


def get_year_totals(year, logger):
    logger.log('\t\t\tGetting ' + str(year) + ' batting totals')
    year_totals = {'R': 0, 'RBI': 0, 'H': 0, '2B': 0, '3B': 0, 'HR': 0, 'SB': 0, 'BB': 0, 'SO': 0}
    db = DatabaseConnection(sandbox_mode)
    pa = int(db.read('select pa from years where year = ' + str(year) + ';')[0][0])
    for key, value in year_totals.items():
        year_totals[key] = float(db.read('select ' + key + ' from years where year=' + str(year) + ';')[0][0])
    db.close()
    return pa, year_totals


# from utilities.logger import Logger
# print(get_year_totals(2018, Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\"
#                                    "dump.log")))
