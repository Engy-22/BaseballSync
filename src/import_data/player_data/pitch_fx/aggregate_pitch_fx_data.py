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
    db = DatabaseConnection()
    db.close()
    driver_logger.log("\t\tTime = " + str(time.time() - start_time))
