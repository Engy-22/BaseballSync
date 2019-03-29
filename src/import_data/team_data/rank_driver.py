import os
import time
import datetime
from urllib.request import urlopen
from bs4 import BeautifulSoup
from utilities.get_oldest_year import get_oldest_year
from import_data.team_data.team_ranker_year import team_ranker_year
from import_data.team_data.team_ranker_ovr import team_ranker_ovr
from statistics import stdev, mean
from utilities.time_converter import time_converter
from utilities.logger import Logger
from utilities.properties import import_driver_logger as driver_logger
from utilities.database.wrappers.baseball_data_connection import DatabaseConnection
from utilities.properties import sandbox_mode, log_prefix

logger = Logger(os.path.join(log_prefix, "import_data", "rank_driver.log"))


def rank_driver(year):
    print("\n\ncalculating team ranks (year)")
    driver_logger.log("\tBeginning rank driver")
    start_time = time.time()
    logger.log("Beginning rank driver || Timestamp: " + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
    logger.log("\tCalculating team ranks (year)")
    runs = {}
    allowed = {}
    difference = {}
    standard_deviation_for = {}
    standard_deviation_against = {}
    standard_deviation_ovr = {}
    ws_winners = {}
    driver_logger.log("\t\tCalculating team ranks (year)")
    for data_year in range(year, get_oldest_year()-1, -1):
        runs[data_year], allowed[data_year], difference[data_year] = team_ranker_year(data_year)
        standard_deviation_for[str(data_year)] = stdev([team_runs_for[1] for team_runs_for in runs[data_year]])
        standard_deviation_against[str(data_year)] = stdev([team_runs_against[1] for team_runs_against in
                                                            allowed[data_year]])
        standard_deviation_ovr[str(data_year)] = stdev([team_runs_diff[1] for team_runs_diff in difference[data_year]])
        ws_winners[data_year] = get_ws_winner(data_year)
    total_time = time_converter(time.time() - start_time)
    logger.log("\t\tTime = " + total_time)
    driver_logger.log("\t\t\tTime = " + total_time)
    second_time = time.time()
    driver_logger.log("\t\tCalculating team ranks (overall)")
    logger.log("\tCalculating team ranks (overall)")
    print("calculating team ranks (overall)")
    total_list = []
    years = [value for key, value in runs.items()]
    for ent in years:
        for team_total in ent:
            total_list.append(team_total[1])
    average_deviation_for = mean([value for key, value in standard_deviation_for.items()])
    average_deviation_against = mean([value for key, value in standard_deviation_against.items()])
    average_deviation_diff = mean([value for key, value in standard_deviation_ovr.items()])
    all_time_rpg = get_all_time_rpg()
    team_ranker_ovr(runs, True, "offRank_ovr", all_time_rpg, standard_deviation_for, average_deviation_for)
    team_ranker_ovr(allowed, False, "defRank_ovr", all_time_rpg, standard_deviation_against, average_deviation_against)
    team_ranker_ovr(difference, True, "ovrRank_ovr", all_time_rpg, standard_deviation_ovr, average_deviation_diff, ws_winners)
    second = time_converter(time.time() - second_time)
    logger.log("\t\tTime = " + second)
    driver_logger.log("\t\t\tTime = " + second)
    total_time = time_converter(time.time() - start_time)
    logger.log("Rank driver complete: time = " + total_time + '\n\n')
    driver_logger.log("\t\tRank driver time = " + total_time)


def get_all_time_rpg():
    page = str(BeautifulSoup(urlopen('https://www.baseball-reference.com/leagues/'), 'html.parser')).\
        split('Major League Historical Totals ')[2]
    runs = ''
    for frag in page.split('<b>Runs</b>: ')[1].split('<BR>')[0].split(','):
        runs += frag
    games = ''
    for frag in page.split('<b>Games</b>: ')[1].split('<BR>')[0].split(','):
        games += frag
    return int(runs) / int(games) / 2


def get_ws_winner(year):
    db = DatabaseConnection(sandbox_mode)
    champ = db.read('select ws_champ from years where year = ' + str(year) + ';')[0][0]
    db.close()
    return champ


# rank_driver(2018)
