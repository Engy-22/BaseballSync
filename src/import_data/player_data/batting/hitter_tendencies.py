import os
import time
import datetime
from urllib.request import urlopen
from bs4 import BeautifulSoup
from utilities.database.wrappers.baseball_data_connection import DatabaseConnection
from utilities.logger import Logger
from utilities.time_converter import time_converter
from utilities.properties import sandbox_mode, log_prefix, import_driver_logger as driver_logger

logger = Logger(os.path.join(log_prefix, "import_data", "hitter_tendecies.log"))


def hitter_tendencies(year):
    print("storing hitter tendencies")
    start_time = time.time()
    logger.log("Downloading " + str(year) + " hitter tendencies || Timestamp: " + datetime.datetime.today()
               .strftime('%Y-%m-%d %H:%M:%S'))
    if year >= 1988:
        driver_logger.log("\tStoring hitter tendencies")
        logger.log("\tDownloading data")
        prev_player_id = ""
        page = str(BeautifulSoup(urlopen('https://www.baseball-reference.com/leagues/MLB/' + str(year)
                                         + '-pitches-batting.shtml'), 'html.parser'))
        table = page.split('<h2>Player Pitches Batting</h2>')[1].split('<tbody>')[1].split('</tbody>')[0]
        rows = table.split('<tr')
        logger.log("\t\tTime = " + time_converter(time.time() - start_time))
        logger.log("\tFormatting data")
        format_time = time.time()
        stat_dictionary = {}
        for row in rows:
            player_id, temp_stats = intermediate(row, prev_player_id)
            if player_id is not None:
                stat_dictionary[player_id] = temp_stats
                prev_player_id = player_id
        for player_id, stats in stat_dictionary.items():
            write_to_file(year, player_id, stats)
        fill_batters_with_0_pa(year)
        total_time = time_converter(time.time() - format_time)
        logger.log("\t\tTime = " + total_time)
        driver_logger.log("\t\tTime = " + total_time)
    else:
        driver_logger.log("\tNo hitter tendency data before 1988")
        logger.log("\tNo hitter tendency data before 1988")
        fill_fields(year)
    logger.log("Done storing hitter tendencies: time = " + time_converter(time.time() - start_time) + '\n\n')


def intermediate(row, prev_player_id):
    stats = {"pitches_per_pa": ["pp_pa", ""],
             "strike_looking_perc": ["strike_looking_percent", ""],
             "strike_swinging_perc": ["strike_swinging_percent", ""],
             "strike_foul_perc": ["strike_foul_percent", ""],
             "strike_inplay_perc": ["strike_in_play_percent", ""],
             "all_strikes_swinging_perc": ["strikes_swung_at_percent", ""],
             "pitches_swinging_perc": ["swing_percent", ""],
             "contact_perc": ["contact_rate", ""],
             "first_pitch_swings_perc": ["first_pitch_swinging_percent", ""],
             "30_pitches": ["counts_30", ""],
             "30_swings": ["swings_30", ""],
             "20_pitches": ["counts_20", ""],
             "20_swings": ["swings_20", ""],
             "31_pitches": ["counts_31", ""],
             "31_swings": ["swings_31", ""],
             "SO_looking_perc": ["k_looking_percent", ""],
             "PA_unknown": ["certainty", ""]}
    try:
        player_id = row.split('data-append-csv="')[1].split('"')[0]
        if player_id == prev_player_id:
            player_id = None
        for key, value in stats.items():
            stats[key][1] = row.split('data-stat="' + key + '" >')[1].split('<')[0].split('%')[0]
            if stats[key][1] == '':
                stats[key][1] = '0'
    except IndexError:
        player_id = None
    return player_id, stats


def write_to_file(year, player_id, stat_list):
    db = DatabaseConnection(sandbox_mode)
    pa = []
    records = []
    pt_uids = list(db.read('select PT_uniqueidentifier from player_teams where playerId = "' + player_id + '";'))
    for pt_uid in pt_uids:
        if len(db.read("select PB_uniqueidentifier from player_batting where PT_uniqueidentifier = " + str(pt_uid[0])
                       + " and year = " + str(year) + ";")) != 0:
            pa.append(int(db.read("select pa from player_batting where pt_uniqueidentifier = " + str(pt_uid[0]) + " and"
                                  " year = " + str(year) + ";")[0][0]))
            records.append(pt_uid)
    if len(pa) != 0:
        for pt in records:
            query_string = ""
            for key, value in stat_list.items():
                if key != "PA_unknown":
                    query_string += value[0] + ' = ' + value[1] + ', '
                else:
                    try:
                        query_string += value[0] + ' = ' + str(pa[0] / (pa[0] + int(value[1]))) + ', '
                    except ZeroDivisionError:
                        query_string += value[0] + ' = ' + '1.0' + ', '
            if len(db.read('select pp_pa from player_batting where PT_uniqueidentifier = ' + str(pt[0])
                           + ' and year = ' + str(year) + ';')):
                db.write('update player_batting set ' + query_string[:-2] + ' where PT_uniqueidentifier = ' + str(pt[0])
                         + ' and year = ' + str(year) + ';')
    db.close()


def fill_fields(year):
    db = DatabaseConnection(sandbox_mode)
    db.write('update player_batting set certainty = 0.0 where year = ' + str(year) + ';')
    db.close()


def fill_batters_with_0_pa(year):
    db = DatabaseConnection(sandbox_mode)
    db.write("update player_batting set certainty = 0.0 where pa = 0 and year = " + str(year) + ";")
    db.close()


# dump_logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\dump.log")
# for year in range(1998, 2009, 1):
#     hitter_tendencies(year, dump_logger)
