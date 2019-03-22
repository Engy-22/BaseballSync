import os
import time
import datetime
from utilities.time_converter import time_converter
from utilities.logger import Logger
from urllib.request import urlopen
from bs4 import BeautifulSoup

logger = Logger(os.path.join("..", "..", "baseball-sync", "logs", "import_data", "triple_crown_winners.log"))


def triple_crown_winners(year, driver_logger):
    driver_logger.log("\tFinding " + str(year) + " triple crown winners")
    start_time = time.time()
    logger.log("Finding " + str(year) + " triple crown winners || Timestamp: " + datetime.datetime.today().\
               strftime('%Y-%m-%d %H:%M:%S'))
    page = str(BeautifulSoup(urlopen('https://www.baseball-reference.com/awards/triple_crowns.shtml'), 'html.parser'))
    batting_table = page.split('Batting Triple Crowns Table')[1].split('</table>')[0]
    pitching_table = page.split('Pitching Triple Crowns Table')[1].split('</table>')[0]
    hitters = get_winners(year, batting_table, "hitting")
    pitchers = get_winners(year, pitching_table, "pitching")
    total_time = time_converter(time.time() - start_time)
    logger.log("Triple crown finder complete: time = " + total_time)
    driver_logger.log("\t\tTime = " + total_time)
    return hitters, pitchers


def get_winners(year, table, player_type):
    rows = table.split('<tr')
    winners = {}
    for row in rows:
        try:
            if str(year) == row.split('.shtml">')[1].split(' ')[0]:
                pass
            else:
                continue
        except IndexError:
            continue
        winners[row.split(str(year) + ' ')[1][:2] + '_triple_crown_' + player_type] = \
            row.split('<td><a href="/players/')[1].split('/')[1].split('.shtml')[0]
    return winners


# triple_crown_winners(2018, Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\"
#                                   "dump.log"))
