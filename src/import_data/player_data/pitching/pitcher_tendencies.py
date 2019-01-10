import os
import time
import datetime
from urllib.request import urlopen
from bs4 import BeautifulSoup
from utilities.DB_Connect import DB_Connect
from utilities.Logger import Logger
from utilities.time_converter import time_converter
from concurrent.futures import ThreadPoolExecutor

logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\"
                "pitcher_tendencies.log")


def pitcher_tendencies(year, driver_logger):
    print("storing pitcher tendencies")
    driver_logger.log("\tStoring pitcher tendencies")
    start_time = time.time()
    logger.log("Downloading " + str(year) + " pitcher tendencies || Timestamp: " + datetime.datetime.today()\
               .strftime('%Y-%m-%d %H:%M:%S'))
    if year >= 1988:
        logger.log("\tDownloading data")
        prev_player_id = ""
        page = str(BeautifulSoup(urlopen('https://www.baseball-reference.com/leagues/MLB/' + str(year)
                                             + '-pitches-pitching.shtml'), 'html.parser'))
        table = page.split('<h2>Player Pitching Pitches</h2>')[1].split('<tbody>')[1].split('</tbody>')[0]
        rows = table.split('<tr')
        logger.log("\t\tTime = " + time_converter(time.time() - start_time))
        logger.log("\tFormatting data")
        format_time = time.time()
        with ThreadPoolExecutor(os.cpu_count()) as executor:
            for row in rows:
                player_id, stats = intermediate(row, prev_player_id)
                if player_id is not None:
                    executor.submit(write_to_file, year, player_id, stats)
                    prev_player_id = player_id
        fill_pitchers_with_0_pa(year)
        logger.log("\t\tTime = " + time_converter(time.time() - format_time))
    else:
        logger.log("\tNo pitcher tendency data before 1988")
        fill_fields(year)
    total_time = time_converter(time.time() - start_time)
    logger.log("Done storing pitcher tendencies: time = " + total_time + '\n\n')
    driver_logger.log("\t\tTime = " + total_time)


def intermediate(row, prev_player_id):
    stats = {"pitches_per_pa": ["pp_pa", ""],
             "strike_looking_perc": ["strike_looking_percent", ""],
             "strike_swinging_perc": ["strike_swinging_percent", ""],
             "strike_foul_perc": ["strike_foul_percent", ""],
             "strike_inplay_perc": ["strike_in_play_percent", ""],
             "all_strikes_swinging_perc": ["strikes_swung_at_percent", ""],
             "pitches_swinging_perc": ["swing_percent", ""],
             "contact_perc": ["contact_rate", ""],
             "first_pitch_strike_perc": ["first_pitch_strike_percent", ""],
             "30_pitches": ["counts_30", ""],
             "30_strikes": ["strikes_30", ""],
             "02_pitches": ["counts_02", ""],
             "02_strikes": ["strikes_02", ""],
             "02_hits": ["hits_02", ""],
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
    db, cursor = DB_Connect.grab("baseballData")
    pa = []
    records = []
    pt_uids = list(DB_Connect.read(cursor, 'select PT_uniqueidentifier from player_teams where playerId = "'
                                           + player_id + '";'))
    for pt_uid in pt_uids:
        if len(DB_Connect.read(cursor, "select PP_uniqueidentifier from player_pitching where PT_uniqueidentifier = "
                                       + str(pt_uid[0]) + " and year = " + str(year) + ";")) != 0:
            pa.append(int(DB_Connect.read(cursor, "select pa from player_pitching where pt_uniqueidentifier = "
                                                  + str(pt_uid[0]) + " and year = " + str(year) + ";")[0][0]))
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
            DB_Connect.write(db, cursor, 'update player_pitching set ' + query_string[:-2] + ' where '
                                         + 'PT_uniqueidentifier = ' + str(pt[0]) + ' and year = ' + str(year) + ';')
    DB_Connect.close(db)


def fill_fields(year):
    db, cursor = DB_Connect.grab("baseballData")
    DB_Connect.write(db, cursor, 'update player_pitching set certainty = 0.0 where year = ' + str(year) + ';')
    DB_Connect.close(db)


def fill_pitchers_with_0_pa(year):
    db, cursor = DB_Connect.grab("baseballData")
    DB_Connect.write(db, cursor, "update player_pitching set certainty = 0.0 where pa = 0 and year = "
                                 + str(year) + ";")
    DB_Connect.close(db)


# pitcher_tendencies(2018, Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\"
#                                 "dump.log"))
