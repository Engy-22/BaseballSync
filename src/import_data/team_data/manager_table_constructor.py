import os
from utilities.time_converter import time_converter
import urllib.request
from bs4 import BeautifulSoup as bs
from concurrent.futures import ThreadPoolExecutor
from utilities.dbconnect import DatabaseConnection
from utilities.logger import Logger
import datetime
import time

logger = Logger('C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\'
                'manager_table_constructor.log')


def manager_table_constructor(driver_logger):
    driver_logger.log('\tGathering manager data (all-time)')
    print("Gathering manager data (all-time)")
    start_time = time.time()
    logger.log('Begin populating teams table || Timestamp: ' + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
    table = str(bs(urllib.request.urlopen('https://www.baseball-reference.com/managers/'), 'html.parser'))
    rows = table.split('<tr')
    with ThreadPoolExecutor(os.cpu_count()) as executor:
        for row in rows:
            if '<td class="left " csk="' in row:
                this_row = row.split('</tr>')[0]
                try:
                    manager_id = this_row.split('<a href="/managers/')[1].split('.shtml')[0].replace("'", "\'")
                    last_first = this_row.split('</tr>')[0].split('<td class="left " csk="')[1].split('"')[0]
                    last = last_first.split(',')[0].replace("'", "\'")
                    first = last_first.split(',')[1].replace("'", "\'")
                    wins = this_row.split('data-stat="W">')[1].split('<')[0]
                    loses = this_row.split('data-stat="L">')[1].split('<')[0]
                    executor.submit(write_to_file, '"' + manager_id + '","' + last + '","' + first + '",' + wins + ','
                                                   + loses)
                except AttributeError:
                    continue
    total_time = time.time() - start_time
    logger.log('Constructing manager table completed: time = ' + time_converter(total_time))
    driver_logger.log('\t\tTime = ' + time_converter(total_time))


def write_to_file(data):
    db = DatabaseConnection()
    db.write('insert into managers (managerId, lastName, firstName, wins, loses) values (' + data + ');')
    db.close()


# manager_table_constructor(Logger('C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\'
#                                  'dump.log'))
