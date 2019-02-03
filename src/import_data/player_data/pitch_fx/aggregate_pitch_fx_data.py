from utilities.connections.baseball_data_connection import DatabaseConnection
from utilities.connections.pitchers_pitch_fx_connection import PitcherPitchFXDatabaseConnection
from utilities.connections.batters_pitch_fx_connection import BatterPitchFXDatabaseConnection
from utilities.logger import Logger
import time
import datetime

logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\logs\\import_data\\"
                "aggregate_pitch_fx_data.log")


def aggregate_pitch_fx_data(year, driver_logger):
    driver_logger.log("\tAggregating pitch fx data")
    start_time = time.time()
    logger.log("Aggregating pitch fx data for " + str(year) + ' || Timestamp: ' + datetime.datetime.today().
               strftime('%Y-%m-%d %H:%M:%S'))
    aggregate(year, 'pitching', PitcherPitchFXDatabaseConnection)
    aggregate(year, 'batting', BatterPitchFXDatabaseConnection)
    total_time = str(time.time() - start_time)
    logger.log("Done aggregating " + str(year) + " pitch fx data: Time = " + total_time)
    driver_logger.log("\t\tTime = " + total_time)


def aggregate(year, player_type, db_connection):
    pitcher_time = time.time()
    logger.log("\tAggregating " + player_type + " data")
    db = DatabaseConnection()
    for player_id in set(db.read('select playerid from ' + player_type[:-3] + 'er_pitches')):
        player_id[0]
    db.close()
    logger.log("\tDone aggregating " + player_type + " data: Time = " + str(time.time() - pitcher_time))


# print(len(aggregate(2008, 'pitching', PitcherPitchFXDatabaseConnection)))
