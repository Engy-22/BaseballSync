import time
import datetime
from utilities.time_converter import time_converter
from utilities.logger import Logger
from utilities.baseball_data_connection import DatabaseConnection
from urllib.request import urlopen
from bs4 import BeautifulSoup

logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\hof_finder.log")


def hof_finder(driver_logger):
    print("adding HOF data")
    driver_logger.log("\tAdding HOF data")
    start_time = time.time()
    logger.log("Begin finding hall of famers || Timestamp: " + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
    hof_table = str(BeautifulSoup(urlopen('https://www.baseball-reference.com/awards/hof.shtml'), 'html.parser')).\
        split('<tbody>')[1].split('</tbody>')[0]
    rows = hof_table.split('<tr>')[1:]
    db = DatabaseConnection()
    for row in rows:
        person = row.split('data-append-csv="')[1].split('"')[0]
        year = row.split('<a href="/awards/hof_')[1].split('.shtml')[0]
        induction_type = row.split('data-stat="category_hof">')[1].split('<')[0]
        if induction_type == 'Player':
            db.write('update players set HOF = ' + str(year)
                                         + ' where playerId = "' + person + '";')
        elif induction_type == 'Manager':
            db.write('update managers set HOF = ' + str(year) + ' where managerId = "' + person + '";')
        else:
            continue
    db.close()
    total_time = time_converter(time.time() - start_time)
    logger.log("Done finding hall of famers: time = " + total_time + '\n\n')
    driver_logger.log("\t\tTime = " + total_time)


# hof_finder(Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\dump.log"))
