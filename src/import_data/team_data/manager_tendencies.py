import os
import time
import datetime
from urllib.request import urlopen
from bs4 import BeautifulSoup
from utilities.connections.baseball_data_connection import DatabaseConnection
from utilities.logger import Logger
from utilities.time_converter import time_converter
from concurrent.futures import ThreadPoolExecutor

logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\manager_tendecies.log")
pages = {}


def manager_tendencies(year, driver_logger, sandbox_mode):
    print("storing manager tendencies")
    start_time = time.time()
    driver_logger.log("\tStoring manager tendencies")
    logger.log("Downloading " + str(year) + " manager tendencies || Timestamp: " + datetime.datetime.today()
               .strftime('%Y-%m-%d %H:%M:%S'))
    db = DatabaseConnection(sandbox_mode)
    managers = db.write('select manager_teams.managerid from manager_teams, manager_year where manager_year.year = '
                        + str(year) + ' and manager_year.mt_uniqueidentifier = manager_teams.mt_uniqueidentifier;')
    db.close()
    with ThreadPoolExecutor(os.cpu_count()) as executor:
        for manager in managers:
            executor.submit(load_url, manager[0])
    total_time = time_converter(time.time() - start_time)
    driver_logger.log("\t\tTime = " + total_time)
    logger.log("Done storing manager tendencies: time = " + total_time + '\n\n')


def load_url(manager_id):
    global pages
    pages[manager_id] = BeautifulSoup(urlopen('https://www.baseball-reference.com/managers/' + manager_id + '.shtml'),
                                      'html.parser')


def write_to_file(manager_id, sandbox_mode):
    db = DatabaseConnection(sandbox_mode)
    db.close()
