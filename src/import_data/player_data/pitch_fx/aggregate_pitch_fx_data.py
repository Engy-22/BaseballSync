from utilities.connections.baseball_data_connection import DatabaseConnection
from utilities.connections.pitchers_pitch_fx_connection import PitcherPitchFXDatabaseConnection
from utilities.connections.batters_pitch_fx_connection import BatterPitchFXDatabaseConnection
from utilities.logger import Logger
import time
import datetime

logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\logs\\import_data\\"
                "aggregate_pitch_fx_data.log")


def aggregate_pitch_fx_data(year, driver_logger, sandbox_mode):
    driver_logger.log("\tAggregating pitch fx data")
    start_time = time.time()
    logger.log("Aggregating pitch fx data for " + str(year) + ' || Timestamp: ' + datetime.datetime.today().
               strftime('%Y-%m-%d %H:%M:%S'))
    aggregate(year, 'pitching', PitcherPitchFXDatabaseConnection, sandbox_mode)
    aggregate(year, 'batting', BatterPitchFXDatabaseConnection, sandbox_mode)
    total_time = str(time.time() - start_time)
    logger.log("Done aggregating " + str(year) + " pitch fx data: Time = " + total_time)
    driver_logger.log("\t\tTime = " + total_time)


def aggregate(year, player_type, db_connection, sandbox_mode):
    pitcher_time = time.time()
    logger.log("\tAggregating " + player_type + " data")
    db = DatabaseConnection(sandbox_mode)
    for player_id in set(db.read('select playerid from ' + player_type[:-3] + 'er_pitches')):
        player_id[0]
    db.close()
    new_db = db_connection()
    logger.log("\tDone aggregating " + player_type + " data: Time = " + str(time.time() - pitcher_time))


# print(len(aggregate(2008, 'pitching', PitcherPitchFXDatabaseConnection)))
