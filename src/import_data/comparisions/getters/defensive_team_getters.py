from utilities.DB_Connect import DB_Connect


def get_defensive_stats(ty_uid, driver_logger):
    stats = {'ER': 0, 'HA': 0, '2BA': 0, '3BA': 0, 'HRA': 0, 'BBA': 0, 'K': 0}
    db, cursor = DB_Connect.grab("baseballData")
    pa = int(DB_Connect.read(cursor, 'select pa from team_years where ty_uniqueidentifier=' + str(ty_uid) + ';')[0][0])
    for key, value in stats.items():
        stats[key] = int(DB_Connect.read(cursor, 'select ' + key + ' from team_years where ty_uniqueidentifier = '
                                                 + str(ty_uid) + ';')[0][0])
    DB_Connect.close(db)
    return pa, stats


def get_year_totals(year, driver_logger):
    year_totals = {'ER': ['ER', 0], 'HA': ['H', 0], '2BA': ['2B', 0], '3BA': ['3B', 0], 'HRA': ['HR', 0],
                   'BBA': ['BB', 0], 'K': ['SO', 0]}
    db, cursor = DB_Connect.grab("baseballData")
    pa = int(DB_Connect.read(cursor, 'select pa from years where year = ' + str(year) + ';')[0][0])
    for key, value in year_totals.items():
        year_totals[key] = \
        DB_Connect.read(cursor, 'select ' + value[0] + ' from years where year=' + str(year) + ';')[0][0]
    DB_Connect.close(db)
    return pa, year_totals
