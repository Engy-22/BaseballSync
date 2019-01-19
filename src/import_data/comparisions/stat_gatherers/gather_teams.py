from utilities.dbconnect import DatabaseConnection


def gather_teams(year, driver_logger):
    db = DatabaseConnection()
    ty_uids = list(db.read('select ty_uniqueidentifier from team_years where year=' + str(year) + ';'))
    db.close()
    return [ty_uid[0] for ty_uid in ty_uids]
