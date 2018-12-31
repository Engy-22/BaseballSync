import os
import time
import datetime
from urllib.request import urlopen, urlretrieve
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from utilities.DB_Connect import DB_Connect
# from utilities.league_batting_ratios_constructor import league_batting_ratios_constructor
# from utilities.translate_team_id import translate_team_id
from utilities.time_converter import time_converter
from utilities.Logger import Logger

pages = {}
reversed_names = {}
logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\batters.log")


def batting_constructor(year, driver_logger):
    print('Downloading batter images and attributes')
    driver_logger.log("\tDownloading batter images and attributes")
    start_time = time.time()
    logger.log("Downloading batter " + str(year) + " data || Timestamp: " + datetime.datetime.today()\
               .strftime('%Y-%m-%d %H:%M:%S'))
    logger.log("\tAssembling list of players")
    table = str(BeautifulSoup(urlopen("https://www.baseball-reference.com/leagues/MLB/" + str(year)
                                      + "-standard-batting.shtml"), 'html.parser')).\
        split('<table class="sortable stats_table" id')[1].split('<tbody>')[1].split('</tbody>')[0].split('<tr')
    player_ids = []
    temp_players = {}
    for row in table:
        if 'data-stat="player" csk="' in row and 'data-append-csv="' in row:
            player_ids.append(row.split('data-append-csv="')[1].split('"')[0].replace("'", "\'"))
            temp_players[player_ids[-1]] = row.split('data-stat="player" csk="')[1].split('" >')[0]
    for player_id, temp_player in temp_players.items():
        if "-0" in temp_player:
            reversed_names[player_id] = temp_player.split("-0")[0].replace("'", "\'")
        else:
            reversed_names[player_id] = temp_player.split("0")[0].replace("'", "\'")
    logger.log("\t\tDone assembling list of players")
    page_time = time.time()
    logger.log("\tGathering player pages")
    with ThreadPoolExecutor(os.cpu_count()) as executor:
        for player_id in player_ids:
            executor.submit(load_url, "https://www.baseball-reference.com/players/" + player_id[0] + "/" + player_id
                                      + ".shtml", player_id)
    logger.log("\t\tTime = " + time_converter(time.time() - page_time))
    logger.log("\tExtracting player attributes and downloading player images")
    extraction_time = time.time()
    for player_id, page in pages.items():
        extract_player_attributes(player_id, page, year, driver_logger)
    logger.log('\t\tTime = ' + time_converter(time.time() - extraction_time))
    total_time = time_converter(time.time() - start_time)
    logger.log("Done downloading player images and attributes: time = " + total_time + '\n\n')
    driver_logger.log("\t\tTime = " + total_time)


def extract_player_attributes(player_id, page, year, driver_logger):
    try:
        for ent in page.find_all('div'):
            str_ent = str(ent)
            if 'Throws: </strong>' in str_ent:
                write_to_db(player_id, {'lastName': reversed_names[player_id].split(',')[1],
                                        'firstName': reversed_names[player_id].split(',')[0],
                                        'batsWith': str_ent.split('Throws: </strong>')[1][0],
                                        'throwsWith': str_ent.split('Bats: </strong>')[1][0],
                                        'primaryPosition': 'N'})
                break
        urlretrieve(str(page.find_all('img')[1]).split('src=')[1].split('/>')[0].split('"')[1],
                    "C:\\Users\\Anthony Raimondo\\images\\players\\" + player_id + ".jpg")
    except MemoryError as e:
        global pages
        del pages
        batting_constructor(year, driver_logger)
        # print('Memory error: just restart this program to get the guys it missed')
        # raise e


def load_url(url, player_id):
    db, cursor = DB_Connect.grab("baseballData")
    if len(DB_Connect.read(cursor, 'select * from players where playerid = "' + player_id + '";')) == 0:
        pages[player_id] = BeautifulSoup(urlopen(url), 'html.parser')
    DB_Connect.close(db)


def write_to_db(player_id, player_attributes):
    fields = ''
    values = ''
    for field, value in player_attributes.items():
        fields += ', ' + field
        values += '", "' + value
    db, cursor = DB_Connect.grab("baseballData")
    if len(DB_Connect.read(cursor, 'select * from players where playerid = "' + player_id + '";')) == 0:
        DB_Connect.write(db, cursor, 'insert into players (playerid ' + fields + ') values ("' + player_id + values
                                     + '");')
    DB_Connect.close(db)


# batting_constructor(2018, Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\"
#                                  "dump.log"))
