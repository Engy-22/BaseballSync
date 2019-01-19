from utilities.dbconnect import DatabaseConnection


def get_offensive_stats(ty_uid, driver_logger):
    stats = {'R': 0, 'H': 0, '2B': 0, '3B': 0, 'HR': 0, 'SB': 0, 'BB': 0, 'SO': 0}
    db = DatabaseConnection()
    pa = int(db.read('select pa from team_years where ty_uniqueidentifier=' + str(ty_uid) + ';')[0][0])
    for key, value in stats.items():
        stats[key] = int(db.read('select ' + key + ' from team_years where ty_uniqueidentifier = ' + str(ty_uid)
                                 + ';')[0][0])
    db.close()
    return pa, stats


def get_year_totals(year, driver_logger):
    year_totals = {'R': 0, 'H': 0, '2B': 0, '3B': 0, 'HR': 0, 'SB': 0, 'BB': 0, 'SO': 0}
    db = DatabaseConnection()
    pa = int(db.read('select pa from years where year = ' + str(year) + ';')[0][0])
    for key, value in year_totals.items():
        year_totals[key] = \
        db.read('select ' + key + ' from years where year=' + str(year) + ';')[0][0]
    db.close()
    return pa, year_totals
