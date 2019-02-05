from utilities.connections.baseball_data_connection import DatabaseConnection


def get_defensive_stats(ty_uid, sandbox_mode):
    stats = {'ER': 0, 'HA': 0, '2BA': 0, '3BA': 0, 'HRA': 0, 'BBA': 0, 'K': 0}
    db = DatabaseConnection(sandbox_mode)
    pa = int(db.read('select pa from team_years where ty_uniqueidentifier=' + str(ty_uid) + ';')[0][0])
    for key, value in stats.items():
        stats[key] = int(db.read('select ' + key + ' from team_years where ty_uniqueidentifier = ' + str(ty_uid)
                                 + ';')[0][0])
    db.close()
    return pa, stats


def get_year_totals(year, logger, sandbox_mode):
    logger.log('\t\t\tGathering ' + str(year) + ' defensive totals')
    year_totals = {'ER': ['ER', 0], 'HA': ['H', 0], '2BA': ['2B', 0], '3BA': ['3B', 0], 'HRA': ['HR', 0],
                   'BBA': ['BB', 0], 'K': ['SO', 0]}
    db = DatabaseConnection(sandbox_mode)
    pa = int(db.read('select pa from years where year = ' + str(year) + ';')[0][0])
    for key, value in year_totals.items():
        year_totals[key] = db.read('select ' + value[0] + ' from years where year=' + str(year) + ';')[0][0]
    db.close()
    return pa, year_totals
