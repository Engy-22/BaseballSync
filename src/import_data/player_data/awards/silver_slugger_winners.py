import os
import time
import datetime
from utilities.time_converter import time_converter
from utilities.logger import Logger
from urllib.request import urlopen
from bs4 import BeautifulSoup

logger = Logger(os.path.join("..", "..", "logs", "import_data", "silver_slugger_winners.log"))


def silver_slugger_winners(year, driver_logger):
    driver_logger.log("\tFinding " + str(year) + " silver sluggers")
    start_time = time.time()
    logger.log("Finding " + str(year) + " silver sluggers || Timestamp: " + datetime.datetime.today().\
               strftime('%Y-%m-%d %H:%M:%S'))
    nl_table = str(BeautifulSoup(urlopen('https://www.baseball-reference.com/awards/silver_slugger_nl.shtml'),
                                 'html.parser')).split('<tbody>')[1].split('</tbody>')[0]
    al_table = str(BeautifulSoup(urlopen('https://www.baseball-reference.com/awards/silver_slugger_al.shtml'),
                                 'html.parser')).split('<tbody>')[1].split('</tbody>')[0]
    nl_silver_sluggers = get_winners(year, nl_table, "NL")
    al_silver_sluggers = get_winners(year, al_table, "AL")
    total_time = time_converter(time.time() - start_time)
    logger.log("Silver slugger finder complete: time = " + total_time)
    driver_logger.log("\t\tTime = " + total_time)
    return nl_silver_sluggers, al_silver_sluggers


def get_winners(year, table, league):
    position_dict = {'1': 'P', '2': 'C', '3': '1B', '4': '2B', '5': '3B', '6': 'SS', '7': 'OF1', '8': 'OF2',
                     '9': 'OF3', '10': 'DH'}
    rows = table.split('<tr>')
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
        if league == "NL":
            count = 0
        else:
            count = 1
        for ent in ents:
            winner = ""
            count += 1
            try:
                for ent2 in ent.split('<span>')[1:]:
                    winner += ent2.split('<a href="/players/')[1].split('/')[1].split('.shtml')[0] + ' & '
                winners[league + '_silver_slugger_' + position_dict[str(count)]] = winner[:-3]
            except KeyError:
                continue
            except IndexError:
                continue
    return winners


# print(silver_slugger_winners(2018, Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\"
#                                           "import_data\\dump.log")))
