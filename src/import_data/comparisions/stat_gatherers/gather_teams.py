from utilities.connections.baseball_data_connection import DatabaseConnection
from utilities.properties import sandbox_mode


def gather_teams(year, logger):
    logger.log('\t\t\tGathering ' + str(year) + ' teams')
    db = DatabaseConnection(sandbox_mode)
    ty_uids = list(db.read('select ty_uniqueidentifier from team_years where year=' + str(year) + ';'))
    db.close()
    return [ty_uid[0] for ty_uid in ty_uids]
