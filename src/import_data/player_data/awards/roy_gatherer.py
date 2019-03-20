import os
import time
import datetime
from utilities.time_converter import time_converter
from utilities.logger import Logger
from urllib.request import urlopen
from bs4 import BeautifulSoup

logger = Logger(os.path.join("..", "..", "logs", "import_data", "roy_gatherer.log"))


def roy_gatherer(year, driver_logger):
    driver_logger.log("\tFinding " + str(year) + " Rookies of the year")
    start_time = time.time()
    logger.log("Finding " + str(year) + " Rookies of the year || Timestamp: " + datetime.datetime.today().\
               strftime('%Y-%m-%d %H:%M:%S'))
    award_winners = {}
    table = str(BeautifulSoup(urlopen('https://www.baseball-reference.com/awards/roy_rol.shtml'), 'html.parser')).\
        split('</tr></thead>')[1].split('</table>')[0]
    rows = table.split('<tr ')
    for row in rows:
        try:
            if str(year) == row.split('valign="top"><td>')[1].split('</td>')[0]:
                pass
            else:
                continue
            if 'NLroy' in row:
                award_winners['nl_roy'] = \
                    row.split('NLroy')[0].split('<a href="/players/')[-1].split('/')[1].split('.shtml"')[0]
            if 'ALroy' in row:
                award_winners['al_roy'] = \
                    row.split('ALroy')[0].split('<a href="/players/')[-1].split('/')[1].split('.shtml"')[0]
        except IndexError:
            continue
    total_time = time_converter(time.time() - start_time)
    logger.log("Rookie of the year finder complete: time = " + total_time)
    driver_logger.log("\t\tTime = " + total_time)
    return award_winners


# print(roy_gatherer(2018, Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\"
#                                 "dump.log")))
