from urllib.request import urlopen, urlretrieve
from bs4 import BeautifulSoup
from utilities.DB_Connect import DB_Connect
# from utilities.league_batting_ratios_constructor import league_batting_ratios_constructor
from utilities.translate_team_id import translate_team_id
import time
import datetime
import os
from utilities.time_converter import time_converter
from utilities.Logger import Logger
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

pages = {}
player_attributes = {}
logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\batters.log")


def batting_constructor(year, driver_logger):
    driver_logger.log("\t")
    start_time = time.time()
    logger.log("Downloading batter " + str(year) + "data || Timestamp: " + datetime.datetime.today()\
               .strftime('%Y-%m-%d %H:%M:%S'))
    player_ids = []
    page_time = time.time()
    logger.log("\tGathering player pages")
    with ThreadPoolExecutor(os.cpu_count()) as executor1:
        for player_id in player_ids:
            executor1.submit(load_url, "https://www.baseball-reference.com/players/" + player_id[0] + "/" + player_id
                             + ".shtml", player_id)
    logger.log("\t\tTime = " + time_converter(time.time() - page_time))
    logger.log("\tExtracting player attributes")
    extraction_time = time.time()
    if __name__ == '__main__':
        extract_player_attributes()
    logger.log('\t\tTime = ' + time_converter(time.time() - extraction_time))
    image_time = time.time()
    logger.log("\tDownloading player images")
    with ThreadPoolExecutor(os.cpu_count()) as executor2:
        for player_id, data in player_attributes:
            executor2.submit(download_image, data, player_id)
    logger.log("\t\tTime = " + time_converter(time.time() - image_time))
    total_time = time_converter(time.time() - start_time)
    logger.log("Done: time = " + total_time + '\n\n')
    driver_logger.log("\t\tTime = " + total_time)


def load_url(url, player_id):
    pages[player_id] = BeautifulSoup(urlopen(url), 'html.parser')


def download_image(image_url, player_id):
    urlretrieve(image_url, "C:\\Users\\Anthony Raimondo\\images\\players\\" + player_id)


def extract_player_attributes():
    with ProcessPoolExecutor(os.cpu_count()) as processor:
        for player_id, page in pages:
            processor.submit(extract, player_id, page)


def extract(player_id, page):
    player_attributes[player_id] = {'pic_url': str(page.find_all('img')[1]).split('src=')[1].split('/>')[0].split('"')[1],
                                    'throwsWith': str(page).split('Throws: </strong>')[1][0],
                                    'batsWith': str(page).split('Bats: </strong>')[1][0]}


def write_to_db(player_id):
    db, cursor = DB_Connect.grab("baseballData")
    if len(DB_Connect.read(cursor, "select * from players where playerid = " + player_id + ";")) == 0:
        DB_Connect.write(db, cursor, 'insert into players (playerid, lastName, firstName, throwsWith, batsWith, '
                                     + 'primaryPosition) values ("' + player_id + '", ");')
    DB_Connect.close(db)


# batting_constructor(2018, Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\"
#                                  "dump.log"))
