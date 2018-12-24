from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from urllib.request import urlopen
import time
import datetime
from bs4 import BeautifulSoup
from utilities.DB_Connect import DB_Connect
from utilities.Logger import Logger

pages = {}
strings = {}
logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\year_data.log")


def get_year_data(year):
    print("Gathering year data")
    start_time = time.time()
    logger.log('\tBeginning year_data download for ' + str(year) + ' || Timestamp: ' + datetime.datetime.today()\
                 .strftime('%Y-%m-%d %H:%M:%S'))
    batting_list = {'PA': 'pa', 'AB': 'ab', 'R': 'r', 'H': 'h', '2B': '2b', '3B': '3b', 'HR': 'hr', 'RBI': 'rbi',
                    'SB': 'sb', 'BB': 'bb', 'SO': 'so', 'batting_avg': 'ba', 'onbase_perc': 'obp',
                    'slugging_perc': 'slg', 'onbase_plus_slugging': 'ops'}
    pitching_list = {'earned_run_avg': 'era', 'whip': 'whip', 'strikeouts_per_nine': 'k_9',
                     'strikeouts_per_base_on_balls': 'k_bb'}
    fielding_list = {'E_def': 'e', 'fielding_perc': 'f_percent'}
    stat_list = {"batting": batting_list, "pitching": pitching_list, "fielding": fielding_list}
    db, cursor = DB_Connect.grab("baseballData")
    if len(DB_Connect.read(cursor, 'select * from years where year = ' + str(year) + ';')) == 0:
        DB_Connect.write(db, cursor, 'insert into years (year) values (' + str(year) + ');')
    DB_Connect.close(db)
    write_opening_day(year)
    download_start = time.time()
    logger.log("\tmaking HTTP requests for year data")
    with ThreadPoolExecutor() as executor1:
        for key, value in stat_list.items():
            executor1.submit(load_url, year, key)
    logger.log("\t\tdone making HTTP requests: time = " + str(round(time.time() - download_start, 2)))
    for key, dictionary in stat_list.items():
        assemble_stats(key, dictionary, pages[key])
    write_start = time.time()
    logger.log("\twriting to database")
    with ThreadPoolExecutor() as executor2:
        for key, value in stat_list.items():
            executor2.submit(write_to_db, year, strings[key], key)
    logger.log("\t\tdone writing to database: time = " + str(round(time.time() - write_start, 2)))
    total_time = round(time.time() - start_time, 2)
    logger.log('\tyear_data download completed: time = ' + str(total_time) + ' seconds\n\n')


def load_url(year, stat_type):
    logger.log("\tdownloading " + stat_type + " data")
    pages[stat_type] = BeautifulSoup(urlopen("https://www.baseball-reference.com/leagues/MLB/" + str(year)
                                             + "-standard-" + stat_type + ".shtml"), 'html.parser')


def write_opening_day(year):
    start_time = time.time()
    db, cursor = DB_Connect.grab("baseballData")
    logger.log('\tGetting date of opening day')
    mlb_schedule = str(BeautifulSoup(urlopen('https://www.baseball-reference.com/leagues/MLB/' + str(year)
                                             + '-schedule.shtml'), 'html.parser'))
    opening_date = mlb_schedule.split('<h3>')[1].split('</h3>')[0]
    months = {'March': '03', 'April': '04'}
    opening_day = months[opening_date.split(', ')[1].split(' ')[0]] + '-' + opening_date.split(', ')[1].split(' ')[1]\
                  .split(',')[0]
    DB_Connect.write(db, cursor, 'update years set opening_day = "' + opening_day + '" where year = ' + str(year) + ';')
    total_time = round(time.time() - start_time, 2)
    DB_Connect.close(db)
    logger.log('\t\tComplete (opening day): time = ' + str(total_time) + ' seconds')


def assemble_stats(stat_type, stats, page):
    page_string = str(page)
    start_time = time.time()
    logger.log('\tAssembling year ' + stat_type + ' stats')
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
    total_time = round(time.time() - start_time, 2)
    logger.log('\t\tCompleted (' + stat_type + '): time = ' + str(total_time) + ' seconds')
    strings[stat_type] = this_string


def write_to_db(year, stat_string, stat_type):
    logger.log("\twriting " + stat_type + " stats to database")
    db, cursor = DB_Connect.grab("baseballData")
    DB_Connect.write(db, cursor, 'update years set ' + stat_string[:-2] + ' where year = ' + str(year) + ';')
    DB_Connect.close(db)


# get_year_data(2018)
