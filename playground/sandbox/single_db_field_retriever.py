from utilities.database.wrappers.baseball_data_connection import DatabaseConnection
from ast import literal_eval


db = DatabaseConnection(sandbox_mode=True)
print(literal_eval(db.read('select team_info from team_years where teamid = "min" and year = 2017;')[0][0])['batter_stats']['doziebr01']['advanced_batting_stats']['trajectory_batting']['vl']['0-0'])#.keys())
db.close()
