import os
import time
import datetime
from utilities.logger import Logger
from urllib.request import urlopen
from bs4 import BeautifulSoup
from utilities.database.wrappers.baseball_data_connection import DatabaseConnection
from utilities.translate_team_id import translate_team_id
from utilities.time_converter import time_converter
from utilities.anomaly_team import anomaly_team
from utilities.properties import sandbox_mode, import_driver_logger as driver_logger

logger = Logger(os.path.join("..", "..", "baseball-sync", "logs", "import_data", "team_offensive_statistics.log"))


def team_offensive_statistics(year):
    driver_logger.log("\tGathering team offensive statistics")
    print('Gathering team offensive statistics')
    start_time = time.time()
    logger.log('Downloading team offensive data for ' + str(year) + ' || Timestamp: ' + datetime.datetime.today().\
               strftime('%Y-%m-%d %H:%M:%S'))
    page = str(BeautifulSoup(urlopen("https://www.baseball-reference.com/leagues/MLB/" + str(year)
                                     + "-standard-batting.shtml"), "html.parser"))
    stats = {'PA': 'PA', 'AB': 'AB', 'R': 'R', 'H': 'H', '2B': '2B', '3B': '3B', 'HR': 'HR', 'RBI': 'RBI', 'SB': 'SB',
             'CS': 'CS', 'BB': 'BB', 'SO': 'SO', 'GIDP': 'GDP', 'HBP': 'HBP', 'SH': 'SH', 'SF': 'SF', 'IBB': 'IBB',
             'G': 'G', 'batting_avg': 'BA', 'onbase_perc': 'OBP', 'slugging_perc': 'SLG', 'onbase_plus_slugging': 'OPS'}
    standard_batting_rows = page.split('Player Standard Batting')[0].split('<h2>Team Standard Batting')[1].\
                                 split('<tbody>')[1].split('</tbody>')[0].split('<tr>')
    extract_data(standard_batting_rows, stats, year)
    total_time = time_converter(time.time() - start_time)
    logger.log("Done donwloading team offensive data for " + str(year) + ': time = ' + total_time + '\n\n')
    driver_logger.log('\t\tTime = ' + total_time)


def extract_data(data, stats, year):
    needed_data = {}
    for datum in data:
        try:
            team_id = translate_team_id(datum.split('<a href="/teams/')[1].split('/')[0], year)
            if len(team_id) == 4:
                team_id = anomaly_team(year)
            needed_data[team_id] = {}
        except IndexError:
            continue
        for stat_key, stat_name in stats.items():
            needed_data[team_id][stat_name] = datum.split('data-stat="' + stat_key + '">')[1].split('<')[0]
    write_to_file(needed_data, year)


def write_to_file(team_data, year):
    db = DatabaseConnection(sandbox_mode)
    for team, data in team_data.items():
        logger.log("\tWriting " + team + " data to database")
        sets = ''
        for field, value in data.items():
            if value != '':
                sets += field + ' = ' + value + ', '
            else:
                continue
        db.write('update team_years set ' + sets[:-2] + ' where teamid = "' + team + '" and year = ' + str(year) + ';')
    db.close()


# dump_logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\dump.log")
# team_offensive_statistics(1880, dump_logger)
