from utilities.DB_Connect import DB_Connect


def get_pitcher_stats(pitcher, year, driver_logger):
    stats = {'IP': 0, 'H': 0, '2B': 0, '3B': 0, 'HR': 0, 'SO': 0, 'BB': 0, 'ER': 0}
    db, cursor = DB_Connect.grab("baseballData")
    pa = int(DB_Connect.read(cursor, 'select player_pitching.pa from player_pitching, player_teams where player_pitchi'
                                     + 'ng.PT_uniqueidentifier = player_teams.PT_uniqueidentifier and player_teams.'
                                     + 'playerId = "' + pitcher + '" and year = ' + str(year) + ';')[0][0])
    for key, value in stats.items():
        stats[key] = float(DB_Connect.read(cursor, 'select player_pitching.' + key + ' from player_pitching, player_tea'
                                                   + 'ms where player_pitching.PT_uniqueidentifier = player_teams.'
                                                   + 'PT_uniqueidentifier and player_teams.playerId = "' + pitcher
                                                   + '" and year = ' + str(year) + ';')[0][0])
    DB_Connect.close(db)
    return pa, stats


def get_year_totals(year, driver_logger):
    year_totals = {'IP': 0, 'H': 0, '2B': 0, '3B': 0, 'HR': 0, 'SO': 0, 'BB': 0, 'ER': 0}
    db, cursor = DB_Connect.grab("baseballData")
    pa = int(DB_Connect.read(cursor, 'select pa from years where year = ' + str(year) + ';')[0][0])
    for key, value in year_totals.items():
        year_totals[key] = float(DB_Connect.read(cursor, 'select ' + key + ' from years where year = ' + str(year)
                                                         + ';')[0][0])
    DB_Connect.close(db)
    return pa, year_totals


# print(get_year_totals(2018))
