import os
import time
import datetime
from utilities.time_converter import time_converter
from utilities.logger import Logger
from urllib.request import urlopen
from bs4 import BeautifulSoup

logger = Logger(os.path.join("..", "..", "baseball-sync", "logs", "import_data", "mvp_cy_young.log"))


def mvp_cy_young(year, driver_logger):
    driver_logger.log("\tFinding " + str(year) + " MVPs and Cy Youngs")
    start_time = time.time()
    logger.log("Finding " + str(year) + " MVPs and Cy Youngs || Timestamp: " + datetime.datetime.today().\
               strftime('%Y-%m-%d %H:%M:%S'))
    table = str(BeautifulSoup(urlopen('https://www.baseball-reference.com/awards/mvp_cya.shtml'), 'html.parser'))
    rows = table.split('</tr></thead>')[1].split('</table>')[0].split('<tr valign=')
    award_winners = {}
    for row in rows:
        try:
            if str(year) == row.split('"top"><td>')[1].split('</td>')[0]:
                pass
            else:
                continue
        except IndexError:
            continue
        if 'NLmvp' in row:
            award_winners['nl_mvp'] = \
                row.split('NLmvp')[0].split('<a href="/players/')[-1].split('/')[1].split('.shtml"')[0]
        if 'ALmvp' in row:
            award_winners['al_mvp'] = \
                row.split('ALmvp')[0].split('<a href="/players/')[-1].split('/')[1].split('.shtml"')[0]
        if 'NLcya' in row:
            award_winners['nl_cyYoung'] = \
                row.split('NLcya')[0].split('<a href="/players/')[-1].split('/')[1].split('.shtml"')[0]
        if 'ALcya' in row:
            award_winners['al_cyYoung'] = \
                row.split('ALcya')[0].split('<a href="/players/')[-1].split('/')[1].split('.shtml"')[0]
    total_time = time_converter(time.time() - start_time)
    logger.log("MVP and Cy Young finder complete: time = " + total_time)
    driver_logger.log("\t\tTime = " + total_time)
    return award_winners


# print(mvp_cy_young(2018, Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\"
#                                 "dump.log")))
