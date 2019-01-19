import time
import datetime
from utilities.time_converter import time_converter
from utilities.logger import Logger
from urllib.request import urlopen
from bs4 import BeautifulSoup

logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\moy_gatherer.log")


def moy_gatherer(year, driver_logger):
    driver_logger.log("\tFinding " + str(year) + " managers of the year")
    start_time = time.time()
    logger.log("Finding " + str(year) + " managers of the year || Timestamp: " + datetime.datetime.today().\
               strftime('%Y-%m-%d %H:%M:%S'))
    award_winners = {}
    table = str(BeautifulSoup(urlopen('https://www.baseball-reference.com/awards/manage.shtml'), 'html.parser')).\
        split('</thead></table></div></div></div></div></div></body></html>')[1].split('<!--')[0]
    rows = table.split('<tr>')
    for row in rows:
        try:
            if str(year) == row.split('<td valign="top">')[1].split('<')[0]:
                pass
            else:
                continue
        except IndexError:
            continue
        award_winners['NL_moy'] = row.split('NLmoy')[0].split('<a href="/managers/')[-1].split('.shtml')[0]
        award_winners['AL_moy'] = row.split('ALmoy')[0].split('<a href="/managers/')[-1].split('.shtml')[0]
    total_time = time_converter(time.time() - start_time)
    logger.log("Manager of the year finder complete: time = " + total_time)
    driver_logger.log("\t\tTime = " + total_time)
    return award_winners


# print(moy_gatherer(2017, Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\"
#                                 "dump.log")))
