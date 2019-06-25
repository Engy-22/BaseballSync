import os
from utilities.database.wrappers.baseball_data_connection import DatabaseConnection


def consolidate_player_stats(p_uid, player_type):
    try:
        with open(os.path.join("..", "background", player_type + "_pitch_fx_tables.csv")) as tables_file:
            tables = tables_file.readlines()
    except FileNotFoundError:
        with open(os.path.join("..", "..", "..", "background", player_type + "_pitch_fx_tables.csv")) as tables_file:
            tables = tables_file.readlines()
    db = DatabaseConnection(sandbox_mode=True)
    for table in tables:
        print(table, db.read('select * from ' + table[:-1] + ' where p_uid = "' + str(p_uid) + '";'))
    db.close()


consolidate_player_stats(621, 'batting')
