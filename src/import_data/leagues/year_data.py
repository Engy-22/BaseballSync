import multiprocessing
import logging
import time
import datetime
from collections import Counter
from bs4 import BeautifulSoup
import urllib.request as requests
from utilities.DB_Connect import DB_Connect

logging.basicConfig(filename="..\\..\\..\\logs\\import_data\\year_data.log", level=logging.DEBUG)


def get_year_data(year):
    start_time = time.time()
    logging.info('\tBeginning year_data download for ' + str(year) + ' || Timestamp: ' + datetime.datetime.today()\
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
    write_opening_day(year, db, cursor)
    # pool = multiprocessing.Pool()
    # num_processes = multiprocessing.cpu_count()
    # for _ in range(num_processes):

    # processes = []
    for key, dictionary in stat_list.items():
        assemble_stats(year, db, cursor, key, dictionary)
        # p = multiprocessing.Process(target=assemble_stats, args=(year, db, cursor, key, dictionary,))
        # p.start()
        # processes.append(p)
    # for process in processes:
    #     process.join()
        # assemble_stats(year, db, cursor, key, dictionary)
    DB_Connect.close(db)
    total_time = round(time.time() - start_time, 2)
    logging.info('\tyear_data download completed: time = ' + str(total_time) + ' seconds\n\n')


def assemble_stats(year, db, cursor, stat_type, stats):
    start_time = time.time()
    logging.info('\tGetting year ' + stat_type + ' stats')
    data = str(BeautifulSoup(requests.urlopen("https://www.baseball-reference.com/leagues/MLB/" + str(year)
                                              + "-standard-" + stat_type + ".shtml"), "html.parser"))
    lat_line2 = data.split('</tbody>')[1].split('</tfoot>')[0]
    this_string = ""
    for i in lat_line2.split('<td'):
        name = i.split('data-stat="')[1].split('">')[0]
        if name in stats:
            this_stat = i.split('">')[1].split('<')[0]
            if len(this_stat) != 0:
                this_string += stats[name] + ' = ' + this_stat + ', '
    if stat_type == 'batting':
        big_table = data.split('</tbody>')[0].split('<tbody>')[1].split('<tr')[1:-1]
        games_list = []
        for row in big_table:
            games_list.append(row.split('data-stat="G">')[1].split('<')[0])
        games = Counter(games_list).most_common(1)[0][0]
        this_string = 'g = ' + str(games) + ', ' + this_string
    DB_Connect.write(db, cursor, 'update years set ' + this_string[:-2] + ' where year = ' + str(year) + ';')
    total_time = round(time.time() - start_time, 2)
    logging.info('\t\tCompleted (' + stat_type + '): time = ' + str(total_time) + ' seconds')


def write_opening_day(year, db, cursor):
    start_time = time.time()
    logging.info('\tGetting date of opening day')
    mlb_schedule = str(BeautifulSoup(requests.urlopen('https://www.baseball-reference.com/leagues/MLB/' + str(year)
                                                      + '-schedule.shtml'), 'html.parser'))
    opening_date = mlb_schedule.split('<h3>')[1].split('</h3>')[0]
    months = {'March': '03', 'April': '04'}
    opening_day = months[opening_date.split(', ')[1].split(' ')[0]] + '-' + opening_date.split(', ')[1].split(' ')[1]\
                  .split(',')[0]
    DB_Connect.write(db, cursor, 'update years set opening_day = "' + opening_day + '" where year = ' + str(year) + ';')
    total_time = round(time.time() - start_time, 2)
    logging.info('\t\tComplete (opening day): time = ' + str(total_time) + ' seconds')


# get_year_data(2018)
