import os
import time
import datetime
from utilities.time_converter import time_converter
from utilities.logger import Logger
from utilities.properties import log_prefix
from urllib.request import urlopen
from bs4 import BeautifulSoup

logger = Logger(os.path.join(log_prefix, "import_data", "gold_glove_winners.log"))


def gold_glove_winners(year, driver_logger):
    driver_logger.log("\tFinding " + str(year) + " gold glove winners")
    start_time = time.time()
    logger.log("Finding " + str(year) + " gold glove winners || Timestamp: " + datetime.datetime.today().\
               strftime('%Y-%m-%d %H:%M:%S'))
    nl_table = str(BeautifulSoup(urlopen('https://www.baseball-reference.com/awards/gold_glove_nl.shtml'),
                                 'html.parser')).split('<tbody>')[1].split('</tbody>')[0]
    al_table = str(BeautifulSoup(urlopen('https://www.baseball-reference.com/awards/gold_glove_al.shtml'),
                                 'html.parser')).split('<tbody>')[1].split('</tbody>')[0]
    nl_gold_glovers = get_winners(year, nl_table, "NL")
    al_gold_glovers = get_winners(year, al_table, "AL")
    total_time = time_converter(time.time() - start_time)
    logger.log("Gold glove finder complete: time = " + total_time)
    driver_logger.log("\t\tTime = " + total_time)
    return nl_gold_glovers, al_gold_glovers


def get_winners(year, table, league):
    position_dict = {'1': 'P', '2': 'C', '3': '1B', '4': '2B', '5': '3B', '6': 'SS', '7': 'OF1', '8': 'OF2', '9': 'OF3'}
    rows = table.split('<tr>')[1:]
    winners = {}
    for row in rows:
        try:
            if str(year) == row.split('name="')[1].split('"')[0]:
                pass
            else:
                continue
        except IndexError:
            continue
        ents = row.split('</td>')[1:]
        count = 0
        for ent in ents:
            winner = ""
            count += 1
            try:
                for ent2 in ent.split('<span>')[1:]:
                    winner += ent2.split('<a href="/players/')[1].split('/')[1].split('.shtml')[0] + ' & '
                winners[league + '_gold_glove_' + position_dict[str(count)]] = winner[:-3]
            except KeyError:
                continue
            except IndexError:
                continue
    return winners


# print(gold_glove_winners(2012, Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\"
#                                       "import_data\\dump.log")))
