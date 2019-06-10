from utilities.database.wrappers.baseball_data_connection import DatabaseConnection


def data_continuity(most_recent_year):
    db = DatabaseConnection(sandbox_mode=True)
    for year in range(most_recent_year, 1875, -1):
        if len(db.read('select year from years where year = ' + str(year) + ';')) == 0:
            continuous = False
            break
        else:
            continuous = True
    db.close()
    return continuous
