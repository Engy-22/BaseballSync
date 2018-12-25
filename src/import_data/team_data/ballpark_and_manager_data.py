from utilities.Logger import Logger
import time
import datetime
from utilities.DB_Connect import DB_Connect
from concurrent.futures import ThreadPoolExecutor
from urllib.request import urlopen
from bs4 import BeautifulSoup


pages = {}
logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\"
                "ballpark_and_manager_data.log")


def ballpark_and_manager_data(year):
    print("Gathering ballpark and manager data")
    start_time = time.time()
    logger.log('Beginning ballpark and manager data download for ' + str(year) + ' || Timestamp: '
               + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
    team_keys = []
    team_ids = []
    with open('C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\background\\yearTeams.txt', 'rt') as file:
        for line in file:
            if str(year) in line:
                temp_line = line.split(',')[1:-1]
                for team in temp_line:
                    temp_team = team.split(';')
                    if 'TOT' not in temp_team:
                        team_keys.append(temp_team[1])
                        team_ids.append(temp_team[0])
                break
    logger.log('Begin downloading team pages')
    download_time = time.time()
    with ThreadPoolExecutor() as executor:
        for team_key in team_keys:
            executor.submit(load_url, year, team_key)
    logger.log('\tDone downloading team pages: time = ' + str(round(time.time() - download_time, 2)))
    logger.log('Ballpark and manager data download completed: time = ' + str(round(time.time() - start_time, 2))
               + ' seconds\n\n')


def load_url(year, team_key):
    logger.log("downloading " + team_key + " data")
    pages[team_key] = BeautifulSoup(urlopen('https://www.baseball-reference.com/teams/split.cgi?t=p&team=' + team_key
                                            + '&year=' + str(year)), 'html.parser')


def write_to_db():
    db, cursor = DB_Connect.grab("baseballData")
    DB_Connect.close(db)


ballpark_and_manager_data(2018)
