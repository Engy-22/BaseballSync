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
from utilities.properties import sandbox_mode, log_prefix, import_driver_logger as driver_logger

logger = Logger(os.path.join(log_prefix, "import_data", "team_defensive_statistics.log"))


def team_defensive_statistics(year):
    driver_logger.log("\tGathering team defensive statistics")
    print('Gathering team defensive statistics')
    start_time = time.time()
    logger.log('Downloading team defensive data for ' + str(year) + ' || Timestamp: ' + datetime.datetime.today().\
               strftime('%Y-%m-%d %H:%M:%S'))
    page1 = str(BeautifulSoup(urlopen("https://www.baseball-reference.com/leagues/MLB/" + str(year)
                                      + "-standard-pitching.shtml"), "html.parser"))
    try:
        page2 = str(BeautifulSoup(urlopen("https://www.baseball-reference.com/leagues/MLB/" + str(year)
                                          + "-batting-pitching.shtml"), "html.parser"))
        batting_against_rows = page2.split('Player Batting Against')[0].split('<h2>Team Batting Against')[1].\
                                     split('<tbody>')[1].split('</tbody>')[0].split('<tr>')
    except Exception:
        batting_against_rows = []
    standard_pitching_rows = page1.split('Player Standard Pitching')[0].split('<h2>Team Standard Pitching')[1].\
                                   split('<tbody>')[1].split('</tbody>')[0].split('<tr>')
    stats1 = {'R': 'RA', 'ER': 'ER', 'H': "HA", 'HR': 'HRA', 'BB': 'BBA', 'HBP': 'HBPA', 'IBB': 'IBBA', 'SO': 'K',
              'ERA': 'ERA', 'whip': 'WHIP'}
    stats2 = {'PA': 'PAA', 'AB': 'ABA', '2B': '2BA', '3B': '3BA', 'batting_avg': 'BAA', 'onbase_perc': 'OBA',
              'slugging_perc': 'SLGA', 'onbase_plus_slugging': 'OPSA', 'batting_avg_bip': 'BABIPA'}
    extract_data(standard_pitching_rows, stats1, year)
    extract_data(batting_against_rows, stats2, year)
    total_time = time_converter(time.time() - start_time)
    logger.log("Done donwloading team defensive data for " + str(year) + ': time = ' + total_time + '\n\n')
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
            if stat_key != 'ERA':
                needed_data[team_id][stat_name] = datum.split('data-stat="' + stat_key + '">')[1].split('<')[0]
            else:
                needed_data[team_id][stat_name] = datum.split('ERA: ')[1].split('&')[0]
    write_to_file(needed_data, year)


def write_to_file(team_data, year,):
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
# team_defensive_statistics(2018, dump_logger)
