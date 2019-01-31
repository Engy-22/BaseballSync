from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from urllib.request import urlopen
from bs4 import BeautifulSoup
import time
import datetime
from utilities.baseball_data_connection import DatabaseConnection
from utilities.logger import Logger
from utilities.time_converter import time_converter

pages = {}
strings = {}
logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\year_data.log")


def get_year_data(year, driver_logger):
    driver_logger.log('\tGathering year data')
    print("Gathering year data")
    start_time = time.time()
    global pages
    global strings
    pages = {}
    strings = {}
    logger.log('Beginning year_data download for ' + str(year) + ' || Timestamp: ' + datetime.datetime.today()
               .strftime('%Y-%m-%d %H:%M:%S'))
    batting_list = {'PA': 'pa', 'AB': 'ab', 'R': 'r', 'H': 'h', '2B': '2b', '3B': '3b', 'HR': 'hr', 'RBI': 'rbi',
                    'SB': 'sb', 'BB': 'bb', 'SO': 'so', 'batting_avg': 'ba', 'onbase_perc': 'obp',
                    'slugging_perc': 'slg', 'onbase_plus_slugging': 'ops'}
    pitching_list = {'earned_run_avg': 'era', 'SV': 'sv', 'IP': 'ip', 'ER': 'er', 'whip': 'whip',
                     'strikeouts_per_nine': 'k_9', 'strikeouts_per_base_on_balls': 'k_bb'}
    fielding_list = {'E_def': 'e', 'fielding_perc': 'f_percent'}
    stat_list = {"batting": batting_list, "pitching": pitching_list, "fielding": fielding_list}
    db = DatabaseConnection()
    if len(db.read('select * from years where year = ' + str(year) + ';')) == 0:
        db.write('insert into years (year) values (' + str(year) + ');')
    db.close()
    write_opening_day(year)
    download_start = time.time()
    logger.log("making HTTP requests for year data")
    with ThreadPoolExecutor(3) as executor1:
        for key, value in stat_list.items():
            executor1.submit(load_url, year, key)
    logger.log("\tdone making HTTP requests: time = " + time_converter(time.time() - download_start))
    for key, dictionary in stat_list.items():
        assemble_stats(key, dictionary, pages[key])
    write_start = time.time()
    logger.log("writing to database")
    with ThreadPoolExecutor(3) as executor2:
        for key, value in stat_list.items():
            executor2.submit(write_to_db, year, strings[key], key)
    logger.log("\tdone writing to database: time = " + time_converter(time.time() - write_start))
    total_time = time_converter(time.time() - start_time)
    logger.log('year_data download completed: time = ' + total_time + '\n\n')
    driver_logger.log('\t\tTime = ' + total_time + ' seconds')


def load_url(year, stat_type):
    logger.log("downloading " + stat_type + " data")
    global pages
    pages[stat_type] = BeautifulSoup(urlopen("https://www.baseball-reference.com/leagues/MLB/" + str(year)
                                             + "-standard-" + stat_type + ".shtml"), 'html.parser')


def write_opening_day(year):
    start_time = time.time()
    db = DatabaseConnection()
    logger.log('Getting date of opening day')
    mlb_schedule = str(BeautifulSoup(urlopen('https://www.baseball-reference.com/leagues/MLB/' + str(year)
                                             + '-schedule.shtml'), 'html.parser'))
    opening_date = mlb_schedule.split('<h3>')[1].split('</h3>')[0]
    months = {'March': '03', 'April': '04', 'May': '05'}
    opening_day = months[opening_date.split(', ')[1].split(' ')[0]] + '-' + opening_date.split(', ')[1].split(' ')[1]\
        .split(',')[0]
    db.write('update years set opening_day = "' + opening_day + '" where year = ' + str(year) + ';')
    db.close()
    logger.log('\tComplete (opening day): time = ' + time_converter(time.time() - start_time))


def assemble_stats(stat_type, stats, page):
    page_string = str(page)
    start_time = time.time()
    logger.log('Assembling year ' + stat_type + ' stats')
    lat_line2 = page_string.split('</tbody>')[1].split('</tfoot>')[0]
    this_string = ""
    for i in lat_line2.split('<td'):
        name = i.split('data-stat="')[1].split('">')[0]
        if name in stats:
            this_stat = i.split('">')[1].split('<')[0]
            if len(this_stat) != 0:
                this_string += stats[name] + ' = ' + this_stat + ', '
    if 'batting' in stat_type:
        big_table = page_string.split('</tbody>')[0].split('<tbody>')[1].split('<tr')[1:-1]
        games_list = []
        for row in big_table:
            games_list.append(row.split('data-stat="G">')[1].split('<')[0])
        games = Counter(games_list).most_common(1)[0][0]
        this_string = 'g = ' + str(games) + ', ' + this_string
    strings[stat_type] = this_string
    logger.log('\tCompleted (' + stat_type + '): time = ' + time_converter(time.time() - start_time))
    strings[stat_type] = this_string


def write_to_db(year, stat_string, stat_type):
    logger.log("writing " + stat_type + " stats to database")
    db = DatabaseConnection()
    db.write('update years set ' + stat_string[:-2] + ' where year = ' + str(year) + ';')
    db.close()


# dump_logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\dump.log")
# for year in range(1996, 2009, 1):
#     get_year_data(year, dump_logger)
