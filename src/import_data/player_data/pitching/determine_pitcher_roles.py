from utilities.database.wrappers.baseball_data_connection import DatabaseConnection
from utilities.properties import sandbox_mode, import_driver_logger as driver_logger
from utilities.time_converter import time_converter
from utilities.logger import Logger
import os
import time
import datetime
from concurrent.futures import ThreadPoolExecutor

logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\"
                "determine_pitcher_roles.log")


def determine_pitcher_roles_year(year):
    driver_logger.log("\tDetermining Pitcher Roles")
    print("Determining Pitcher Roles")
    start_time = time.time()
    logger.log("Determining Pitcher Roles || Timestamp: " + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
    db = DatabaseConnection(sandbox_mode)
    with ThreadPoolExecutor(os.cpu_count()) as executor:
        for pt_uid in db.read('select pt_uniqueidentifier from player_pitching where year = ' + str(year) + ';'):
            player_id_team_id = db.read('select playerid, teamid from player_teams where pt_uniqueidentifier='
                                        + str(pt_uid[0]) + ';')[0]
            player_id = player_id_team_id[0]
            team_id = player_id_team_id[1]
            if team_id == 'TOT':
                continue
            ty_uid = str(db.read('select ty_uniqueidentifier from team_years where teamId = "' + team_id
                                 + '" and year = ' + str(year) + ';')[0][0])
            try:
                positions = db.read('select positions from player_positions where playerId = "' + player_id + '" and '
                                    'ty_uniqueidentifier = ' + ty_uid + ';')[0][0]
            except IndexError:
                continue
            update_positions = []
            if 'P' in positions:
                appearances_starts = db.read('select G, GS from player_pitching where pt_uniqueidentifier = '
                                             + str(pt_uid[0]) + ' and year = ' + str(year) + ';')[0]
                appearances = appearances_starts[0]
                starts = appearances_starts[1]
                start_percent = starts / appearances
                if start_percent > 0.75:
                    role = ['SP']
                elif start_percent > 0.50:
                    role = ['SP', 'RP']
                elif start_percent > 0.25:
                    role = ['RP', 'SP']
                else:
                    role = ['RP']
                for position in positions.split(','):
                    if position == 'P':
                        update_positions += role
                    else:
                        update_positions.append(position)
                executor.submit(db.write('update player_positions set positions = "' + ','.join(update_positions)
                                         + '" where ty_uniqueidentifier = ' + ty_uid + ' and playerId = "' + player_id
                                         + '";'))
    db.close()
    total_time = time_converter(time.time() - start_time)
    logger.log("Done: Time = " + total_time + '\n\n')
    driver_logger.log("\t\tTIme = " + total_time)


# determine_pitcher_roles_year(1971)
