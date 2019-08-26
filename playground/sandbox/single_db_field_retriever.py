from utilities.database.wrappers.baseball_data_connection import DatabaseConnection
from ast import literal_eval


db = DatabaseConnection(sandbox_mode=True)
print(literal_eval(db.read('select year_info from years where year = 2017;')[0][0])['league_pitching_stats'].keys())
db.close()
