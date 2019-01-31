import os
from utilities.dbconnect import DatabaseConnection
from utilities.logger import Logger
import time
import datetime
from concurrent.futures import ThreadPoolExecutor

logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\logs\\import_data\\"
                "aggregate_pitch_fx_data.log")


def aggregate_pitch_fx_data(year, driver_logger):
    driver_logger.log("\tAggregating pitch fx data")
    start_time = time.time()
    logger.log("Aggregating pitch fx data for " + str(year) + ' || Timestamp: ' + datetime.datetime.today().
               strftime('%Y-%m-%d %H:%M:%S'))
    pitcher_time = time.time()
    logger.log("\tAggregating pitching data")

    logger.log("\tDone aggregating pitching data: Time = " + str(time.time() - pitcher_time))
    batter_time = time.time()
    logger.log("\tAggregating batting data")
    
    logger.log("\tDone aggregating batting data: Time = " + str(time.time() - batter_time))
    db = DatabaseConnection()
    db.close()
    total_time = str(time.time() - start_time)
    logger.log("Done aggregating " + str(year) + " pitch fx data: Time = " + total_time)
    driver_logger.log("\t\tTime = " + total_time)
