import os
import time
import datetime
from urllib.request import urlopen
from bs4 import BeautifulSoup
from utilities.database.wrappers.baseball_data_connection import DatabaseConnection
from utilities.logger import Logger
from utilities.time_converter import time_converter
from concurrent.futures import ThreadPoolExecutor
from utilities.properties import sandbox_mode, import_driver_logger as driver_logger

logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\manager_tendecies.log")
pages = {}
stats = {}


def manager_tendencies(year):
    driver_logger.log("\tStoring manager tendencies")
    print("storing manager tendencies")
    start_time = time.time()
    logger.log("Downloading " + str(year) + " manager tendencies || Timestamp: " + datetime.datetime.today()
               .strftime('%Y-%m-%d %H:%M:%S'))
    logger.log('\tMaking HTTP requests')
    db = DatabaseConnection(sandbox_mode)
    managers = db.read('select manager_teams.managerid, manager_teams.teamid from manager_teams, manager_year where '
                       'manager_year.year = ' + str(year) + ' and manager_year.mt_uniqueidentifier = manager_teams.'
                       'mt_uniqueidentifier;')
    db.close()
    with ThreadPoolExecutor(os.cpu_count()) as executor:
        for manager in managers:
            executor.submit(load_url, manager[0], manager[1])
    logger.log('\t\tTime = ' + time_converter(time.time() - start_time))
    process_manager_tendencies(year)
    write_time = time.time()
    logger.log('\tWriting data to database')
    global stats
    with ThreadPoolExecutor(os.cpu_count()) as executor2:
        for manager_team, tendencies in stats.items():
            if len(tendencies) > 0:
                executor2.submit(write_to_file, year, manager_team, tendencies)
    logger.log('\t\tTime = ' + time_converter(time.time() - write_time))
    total_time = time_converter(time.time() - start_time)
    driver_logger.log("\t\tTime = " + total_time)
    logger.log("Done storing manager tendencies: time = " + total_time + '\n\n')


def load_url(manager_id, team_id):
    global pages
    pages[manager_id + ';' + team_id] = BeautifulSoup(urlopen('https://www.baseball-reference.com/managers/'
                                                              + manager_id + '.shtml'), 'html.parser')


def process_manager_tendencies(year):
    start_time = time.time()
    logger.log('\tProcessing manager tendencies')
    global pages
    global stats
    for manager_team, tendencies in pages.items():
        stats[manager_team] = {}
        stats_to_consider = ['steal_2b_chances', 'steal_2b_attempts', 'steal_3b_chances', 'steal_3b_attempts',
                             'sac_bunt_chances', 'sac_bunts', 'ibb_chances', 'ibb', 'pinch_hitters', 'pinch_runners',
                             'pitchers_used_per_game']
        try:
            rows = str(tendencies).split('<h2>Managerial Tendencies</h2>')[1].split('tbody>')[1].split('<tr>')
            for row in rows:
                try:
                    if row.split('.shtml">')[1].split('</a>')[0] == str(year):
                        for stat in stats_to_consider:
                            for datum in row.split('<td'):
                                if stat in datum:
                                    stats[manager_team][stat] = row.split('data-stat="' + stat + '">')[1].split('</td>')[0]
                                    break
                        break
                except IndexError:
                    continue
        except IndexError:
            continue
    logger.log('\t\tTime = ' + time_converter(time.time() - start_time))


def write_to_file(year, manager_team, tendencies):
    db = DatabaseConnection(sandbox_mode)
    sets = ''
    for stat, total in tendencies.items():
        sets += stat + ' = ' + total + ', '
    db.write('update manager_year set' + sets[:-2] + ' where year = ' + str(year) + ' and mt_uniqueidentifier = '
             '(select mt_uniqueidentifier from manager_teams where managerid = "' + manager_team.split(';')[0]
             + '" and teamid = "' + manager_team.split(';')[1] + '");')
    db.close()


# manager_tendencies(2008, Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\"
#                                 "dump.log"), False)
