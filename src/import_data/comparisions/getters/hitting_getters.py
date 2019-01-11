from utilities.DB_Connect import DB_Connect


def get_hitter_stats(hitter, year, driver_logger):
    stats = {'R': 0, 'RBI': 0, 'H': 0, '2B': 0, '3B': 0, 'HR': 0, 'SB': 0, 'BB': 0, 'SO': 0}
    db, cursor = DB_Connect.grab("baseballData")
    pa = int(DB_Connect.read(cursor, 'select player_batting.pa from player_batting, player_teams where '
                                     + 'player_batting.PT_uniqueidentifier = player_teams.PT_uniqueidentifier and '
                                     + 'player_teams.playerId = "' + hitter + '" and year = ' + str(year) + ';')[0][0])
    for key, value in stats.items():
        stats[key] = int(DB_Connect.read(cursor, 'select player_batting.' + key + ' from player_batting, player_teams'
                                                 + ' where player_batting.PT_uniqueidentifier ='
                                                 + ' player_teams.PT_uniqueidentifier and player_teams.playerId = "'
                                                 + hitter + '" and year = ' + str(year) + ';')[0][0])
    DB_Connect.close(db)
    return pa, stats


def get_year_totals(year, driver_logger):
    year_totals = {'R': 0, 'RBI': 0, 'H': 0, '2B': 0, '3B': 0, 'HR': 0, 'SB': 0, 'BB': 0, 'SO': 0}
    db, cursor = DB_Connect.grab("baseballData")
    pa = int(DB_Connect.read(cursor, 'select pa from years where year = ' + str(year) + ';')[0][0])
    for key, value in year_totals.items():
        year_totals[key] = DB_Connect.read(cursor, 'select ' + key + ' from years where year=' + str(year) + ';')[0][0]
    DB_Connect.close(db)
    return pa, year_totals


# print(get_year_totals(2018))
