import time
import datetime
from urllib.request import urlopen
from bs4 import BeautifulSoup
from utilities.connections.baseball_data_connection import DatabaseConnection
from utilities.logger import Logger
from utilities.time_converter import time_converter

logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\manager_tendecies.log")


def manager_tendencies(year, driver_logger, sandbox_mode):
    print("storing manager tendencies")
    start_time = time.time()
    logger.log("Downloading " + str(year) + " manager tendencies || Timestamp: " + datetime.datetime.today()
               .strftime('%Y-%m-%d %H:%M:%S'))

    logger.log("Done storing manager tendencies: time = " + time_converter(time.time() - start_time) + '\n\n')
