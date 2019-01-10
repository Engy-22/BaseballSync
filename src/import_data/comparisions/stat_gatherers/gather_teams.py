from utilities.DB_Connect import DB_Connect


def gather_teams(year):
    db, cursor = DB_Connect.grab("baseballData")
    ty_uids = list(DB_Connect.read(cursor, 'select ty_uniqueidentifier from team_years where year=' + str(year) + ';'))
    DB_Connect.close(db)
    return [ty_uid[0] for ty_uid in ty_uids]
