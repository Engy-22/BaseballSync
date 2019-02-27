from utilities.database.wrappers.baseball_data_connection import DatabaseConnection
from import_data.consolidata.team_roster_info import consolidate_hitter_spots, consolidate_player_positions, write_roster_info
from utilities.logger import Logger
from utilities.properties import sandbox_mode, import_driver_logger as driver_logger
from utilities.time_converter import time_converter
import datetime
import time

logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\consolidata.log")


def consolidate_data():
    # year = [2016]
    driver_logger.log("\tConsolidating data")
    print("Consolidating data")
    start_time = time.time()
    logger.log("Consolidating team data || Timestamp: " + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
    db = DatabaseConnection(sandbox_mode)
    for year in db.read('select year from years;'):
        for ty_uid in db.read('select ty_uniqueidentifier from team_years where year = ' + str(year[0]) + ';'):
            write_roster_info(ty_uid[0], {'hitter_spots': consolidate_hitter_spots(ty_uid[0]),
                                          'player_positions': consolidate_player_positions(ty_uid[0])})
    db.close()
    total_time = time_converter(time.time() - start_time)
    logger.log("Done consolidating team data: Time = " + total_time)
    driver_logger.log("\t\tTime = " + total_time)


# consolidate_data()
