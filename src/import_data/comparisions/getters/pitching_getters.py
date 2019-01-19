from utilities.dbconnect import DatabaseConnection


def get_pitcher_stats(pitcher, year, driver_logger):
    stats = {'IP': 0, 'H': 0, '2B': 0, '3B': 0, 'HR': 0, 'SO': 0, 'BB': 0, 'ER': 0}
    db = DatabaseConnection()
    pa = int(db.read('select player_pitching.pa from player_pitching, player_teams where player_pitching.'
                     'PT_uniqueidentifier = player_teams.PT_uniqueidentifier and player_teams.playerId = "' + pitcher
                     + '" and year = ' + str(year) + ';')[0][0])
    for key, value in stats.items():
        try:
            stats[key] = float(db.read('select player_pitching.' + key + ' from player_pitching, player_teams where '
                                       'player_pitching.PT_uniqueidentifier = player_teams.PT_uniqueidentifier and '
                                       'player_teams.playerId = "' + pitcher + '" and year = ' + str(year) + ';')[0][0])
        except TypeError:
            pa = 0
            break
    db.close()
    return pa, stats


def get_year_totals(year, driver_logger):
    year_totals = {'IP': 0, 'H': 0, '2B': 0, '3B': 0, 'HR': 0, 'SO': 0, 'BB': 0, 'ER': 0}
    db = DatabaseConnection()
    pa = int(db.read('select pa from years where year = ' + str(year) + ';')[0][0])
    for key, value in year_totals.items():
        year_totals[key] = float(db.read('select ' + key + ' from years where year = ' + str(year) + ';')[0][0])
    db.close()
    return pa, year_totals


# from utilities.Logger import Logger
# print(get_year_totals(2018, Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\"
#                                    "dump.log")))
