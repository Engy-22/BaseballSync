import os
import time
import datetime
from urllib.request import urlopen
from bs4 import BeautifulSoup
from utilities.DB_Connect import DB_Connect
from utilities. Logger import Logger
from utilities.time_converter import time_converter
from concurrent.futures import ThreadPoolExecutor

pages = {}
logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\"
                "team_batting_order_constructor.log")


def team_batting_order_constructor(year, driver_logger):
    if year >= 1908:
        print("getting team batting order data")
        driver_logger.log("\tGetting team batting order data")
        start_time = time.time()
        logger.log("Downloading " + str(year) + " team batting order data || Timestamp: " + datetime.datetime.today()\
                   .strftime('%Y-%m-%d %H:%M:%S'))
        logger.log("\tDownloading team pages")
        with open("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\background\\yearTeams.txt", 'r') as year_file:
            with ThreadPoolExecutor(os.cpu_count()) as executor:
                for line in year_file:
                    if str(year) in line:
                        temp_line = line.split(',')[1:-1]
                        for team in temp_line:
                            if "TOT" not in team:
                                executor.submit(load_url, year, team.split(';')[0], team.split(';')[1])
                        break
        logger.log("\t\tTime = " + time_converter(time.time() - start_time))
        logger.log("\tOrganizing batting orders")
        write_time = time.time()
        get_hitters(year)
        logger.log("\t\tTime = " + time_converter(time.time() - write_time))
        total_time = time_converter(time.time() - start_time)
        logger.log("Done downloading team batting order data: time = " + total_time + '\n\n')
        driver_logger.log("\t\tTime = " + total_time)
    else:
        logger.log("\tNo team batting order data to download. (before 1908).")
        driver_logger.log("\tNo team batting order data to download. (before 1908).")


def load_url(year, team_id, team_key):
    logger.log("\t\tdownloading " + team_id + " page")
    pages[team_id] = str(BeautifulSoup(urlopen("https://www.baseball-reference.com/teams/" + team_key + "/" + str(year)
                                               + "-batting-orders.shtml"), 'html.parser'))\
        .split('Most-<small>Games</small>')[0].split('tbody>')[1].split('<tr')


def get_hitters(year):
    for team_id, children in pages.items():
        logger.log("\tGetting " + team_id + " batting order data")
        temp_order = []
        order = [[], [], [], [], [], [], [], [], []]
        final_order = [[], [], [], [], [], [], [], [], []]
        order_count = [[], [], [], [], [], [], [], [], []]
        try:
            counter = 0
            for child in children:
                temp_order.append([])
                individuals = child.split('data-entry-id="')
                for indy in individuals:
                    if '><th class=' not in indy:
                        temp_order[counter].append(indy.split('"')[0].replace("'", "\'"))
                counter += 1
            del temp_order[0]
            for i in range(len(temp_order)):
                for j in range(9):
                    order[j].append(temp_order[i][j])
            for k in range(len(order)):
                order[k].sort()
                for m in range(len(order[k])):
                    if order[k][m] in final_order[k]:
                        order_count[k][-1] += 1
                    else:
                        final_order[k].append(order[k][m])
                        order_count[k].append(1)
            batting_order = []
            for n in range(len(final_order)):
                batting_order.append([])
                for p in range(len(final_order[n])):
                    batting_order[n].append(final_order[n][p] + ',' + str(order_count[n][p]))
            write_to_file(team_id, year, batting_order)
        except IndexError:
            pass


def write_to_file(team, year, batting_order):
    db, cursor = DB_Connect.grab("baseballData")
    for place in range(len(batting_order)):
        for hitter in batting_order[place]:
            ty_uid = DB_Connect.read(cursor, 'select TY_uniqueidentifier from team_years where teamId = "' + team
                                             + '" and year = ' + str(year))[0][0]
            if len(DB_Connect.read(cursor, 'select * from hitter_spots where playerId = "' + hitter.split(',')[0]
                                           + '" and TY_uniqueidentifier = ' + str(ty_uid))) > 0:
                DB_Connect.write(db, cursor, 'update hitter_spots set ' + stringify_num(place) + ' = '
                                             + hitter.split(',')[1] + ' where playerId = "' + hitter.split(',')[0]
                                             + '" and TY_uniqueidentifier = (select TY_uniqueidentifier from team_years'
                                             + ' where teamId = "' + team + '" and year = ' + str(year) + ');')
            else:
                DB_Connect.write(db, cursor, 'insert into hitter_spots (HS_uniqueidentifier, playerId, '
                                             + stringify_num(place) + ', TY_uniqueidentifier) values (default, "'
                                             + hitter.split(',')[0] + '", ' + hitter.split(',')[1] + ', ' + str(ty_uid)
                                             + ');')
    DB_Connect.close(db)


def stringify_num(num):
    dictionary = ['one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine']
    return dictionary[num]


# dump_logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\dump.log")
# team_batting_order_constructor(2018, dump_logger)
