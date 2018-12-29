from urllib.request import urlopen, urlretrieve
from bs4 import BeautifulSoup
from utilities.DB_Connect import DB_Connect
from utilities.translate_team_id import translate_team_id
import time
import datetime
import os
from utilities.time_converter import time_converter
from utilities.Logger import Logger
from concurrent.futures import ThreadPoolExecutor

pages = {}
logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\batters.log")


def fielding_constructor(year, driver_logger):
    driver_logger.log("\t")
    start_time = time.time()
    logger.log("Downloading fielder " + str(year) + "data || Timestamp: " + datetime.datetime.today()\
               .strftime('%Y-%m-%d %H:%M:%S'))
    player_ids = []
    page_time = time.time()
    logger.log("\tGathering player pages")
    with ThreadPoolExecutor(os.cpu_count()) as executor1:
        for player_id in player_ids:
            executor1.submit(load_url, "https://www.baseball-reference.com/players/" + player_id[0] + "/" + player_id
                             + ".shtml", player_id)
    logger.log("\t\tTime = " + time_converter(time.time() - page_time))
    image_time = time.time()
    logger.log("\tGathering player images")
    pic_urls = {}
    with ThreadPoolExecutor(os.cpu_count()) as executor2:
        for player_id, pic_url in pic_urls:
            executor2.submit(download_image, pic_url, player_id)
    logger.log("\t\tTime = " + time_converter(time.time() - image_time))
    total_time = time_converter(time.time() - start_time)
    logger.log("Done: time = " + total_time + '\n\n')
    driver_logger.log("\t\tTime = " + total_time)


def load_url(url, player_id):
    pages[player_id] = BeautifulSoup(urlopen(url), 'html.parser')


def download_image(image_url, player_id):
    urlretrieve(image_url, "C:\\Users\\Anthony Raimondo\\images\\players\\" + player_id)


def write_to_db(player_id):
    db, cursor = DB_Connect.grab("baseballData")
    if len(DB_Connect.read(cursor, "select * from players where playerid = " + player_id + ";")) == 0:
        DB_Connect.write(db, cursor, 'insert into players (playerid, lastName, firstName, throwsWith, batsWith, '
                                     + 'primaryPosition) values ("' + player_id + '", ");')
    DB_Connect.close(db)


# fielding_constructor(2018, Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\"
#                                  "dump.log"))
