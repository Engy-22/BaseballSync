import os
import time
import datetime
from urllib.request import urlopen
from bs4 import BeautifulSoup
from utilities.database.wrappers.baseball_data_connection import DatabaseConnection
from utilities. logger import Logger
from utilities.time_converter import time_converter
from concurrent.futures import ThreadPoolExecutor
from utilities.properties import log_prefix, import_driver_logger as driver_logger
from utilities.num_to_word import num_to_word

pages = {}
logger = Logger(os.path.join(log_prefix, "import_data", "team_batting_order_constructor.log"))


def team_batting_order_constructor(year):
    if year < 1908:
        logger.log("\tNo team batting order data to download before 1908.")
        driver_logger.log("\tNo team batting order data to download before 1908.")
        return
    print("getting team batting order data")
    driver_logger.log("\tGetting team batting order data")
    start_time = time.time()
    global pages
    pages = {}
    logger.log("Downloading " + str(year) + " team batting order data || Timestamp: " + datetime.datetime.today()
               .strftime('%Y-%m-%d %H:%M:%S'))
    logger.log("\tDownloading team pages")
    try:
        year_file = open(os.path.join("..", "background", "yearTeams.txt"), 'r')
    except FileNotFoundError:
        year_file = open(os.path.join("..", "..", "..", "background", "yearTeams.txt"), 'r')
    with ThreadPoolExecutor(os.cpu_count()) as executor:
        for line in year_file:
            if str(year) in line:
                temp_line = line.split(',')[1:-1]
                for team in temp_line:
                    if "TOT" not in team:
                        executor.submit(load_url, year, team.split(';')[0], team.split(';')[1])
                break
    logger.log("\t\t\tTime = " + time_converter(time.time() - start_time))
    logger.log("\tOrganizing batting orders")
    write_time = time.time()
    get_hitters(year)
    logger.log("\t\t\tTime = " + time_converter(time.time() - write_time))
    total_time = time_converter(time.time() - start_time)
    logger.log("Done downloading team batting order data: time = " + total_time + '\n\n')
    driver_logger.log("\t\tTime = " + total_time)


def load_url(year, team_id, team_key):
    logger.log("\t\tdownloading " + team_id + " page")
    pages[team_id] = str(BeautifulSoup(urlopen("https://www.baseball-reference.com/teams/" + team_key + "/" + str(year)
                                               + "-batting-orders.shtml"), 'html.parser'))\
        .split('Most-<small>Games</small>')[0].split('tbody>')[1].split('<tr')


def get_hitters(year):
    for team_id, children in pages.items():
        logger.log("\t\tGetting " + team_id + " batting order data")
        temp_order = {'vr': [], 'vl': []}
        order = {'vr': [[], [], [], [], [], [], [], [], []], 'vl': [[], [], [], [], [], [], [], [], []]}
        final_order = {'vr': [[], [], [], [], [], [], [], [], []], 'vl': [[], [], [], [], [], [], [], [], []]}
        order_count = {'vr': [[], [], [], [], [], [], [], [], []], 'vl': [[], [], [], [], [], [], [], [], []]}
        try:
            r_counter = 0
            l_counter = 0
            for child in children:
                if ')#</th><td class="' in child:
                    matchup = 'vl'
                    l_counter += 1
                else:
                    matchup = 'vr'
                    r_counter += 1
                temp_order[matchup].append([])
                individuals = child.split('data-entry-id="')
                for indy in individuals:
                    if '><th class=' not in indy:
                        temp_order[matchup][r_counter-1 if matchup == 'vr' else l_counter-1].append(indy.split('"')[0].
                                                                                                    replace("'", "\'"))
            del temp_order['vr'][0]
            del temp_order['vl'][0]
            for matchup2, temp_order_by_matchup in temp_order.items():
                for i in range(len(temp_order_by_matchup)):
                    for j in range(9):
                        order[matchup2][j].append(temp_order_by_matchup[i][j])
                for k in range(len(order[matchup2])):
                    order[matchup2][k].sort()
                    for m in range(len(order[matchup2][k])):
                        if order[matchup2][k][m] in final_order[matchup2][k]:
                            order_count[matchup2][k][-1] += 1
                        else:
                            final_order[matchup2][k].append(order[matchup2][k][m])
                            order_count[matchup2][k].append(1)
                batting_order = []
                for n in range(len(final_order[matchup2])):
                    batting_order.append([])
                    for p in range(len(final_order[matchup2][n])):
                        batting_order[n].append(final_order[matchup2][n][p] + ',' + str(order_count[matchup2][n][p]))
                write_to_file(team_id, year, batting_order, matchup2)
        except IndexError:
            continue


def write_to_file(team, year, batting_order, matchup):
    with ThreadPoolExecutor(os.cpu_count()) as executor2:
        for place in range(len(batting_order)):
            for hitter in batting_order[place]:
                executor2.submit(transact, hitter, team, year, place, matchup)


def transact(hitter, team, year, place, matchup):
    db = DatabaseConnection(sandbox_mode=True)
    ty_uid = db.read('select TY_uniqueidentifier from team_years where teamId = "' + team + '" and year = '
                     + str(year))[0][0]
    if len(db.read('select * from hitter_spots where playerId = "' + hitter.split(',')[0] + '" and matchup = "' + matchup
                   + '" and TY_uniqueidentifier = ' + str(ty_uid) + ';')) > 0:
        db.write('update hitter_spots set ' + num_to_word(place+1) + ' = ' + hitter.split(',')[1] + ' where playerId '
                 '= "' + hitter.split(',')[0] + '" and matchup = "' + matchup + '" and TY_uniqueidentifier = (select '
                 'TY_uniqueidentifier from team_years where teamId = "' + team + '" and year = ' + str(year) + ');')
    else:
        db.write('insert into hitter_spots (HS_uniqueidentifier, playerId, matchup, ' + num_to_word(place+1)
                 + ', TY_uniqueidentifier) values (default, "' + hitter.split(',')[0] + '", "' + matchup + '", '
                 + hitter.split(',')[1] + ', ' + str(ty_uid) + ');')
    db.close()


# team_batting_order_constructor(2017)
