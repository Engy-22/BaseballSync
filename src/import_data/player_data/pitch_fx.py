import os
import time
import datetime
from bs4 import BeautifulSoup
# from utilities.translate_pitchfx import translate_pitch_type, translate_pitch_outcome, determine_swing_or_take
from utilities.Logger import Logger
from utilities.DB_Connect import DB_Connect
from utilities.time_converter import time_converter
from concurrent.futures import ThreadPoolExecutor
from urllib.request import urlopen
from bs4 import BeautifulSoup

logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\logs\\import_data\\pitch_fx.log")


def get_pitch_fx_data(year, driver_logger):
    driver_logger.log("\tFetching " + str(year) + " pitch fx data")
    print("Fetching " + str(year) + " pitch fx data")
    start_time = time.time()
    logger.log("Downloading pitch fx data for " + str(year) + ' || Timestamp: ' + datetime.datetime.today().\
               strftime('%Y-%m-%d %H:%M:%S'))
    db, cursor = DB_Connect.grab("baseballData")
    opening_day = DB_Connect.read(cursor, 'select opening_day from years where year = ' + str(year) + ';')[0][0]
    DB_Connect.close(db)
    for month in range(3, 12, 1):
        if month > 3:
            if month < int(opening_day.split('-')[0]):
                continue
            for day in range(1, 32, 1):
                if month == int(opening_day.split('-')[0]) and int(day) < int(opening_day.split('-')[1]):
                    continue
                if len(str(day)) == 1:
                    this_day = '0' + str(day)
                else:
                    this_day = str(day)
                if len(str(month)) == 1:
                    this_month = '0' + str(month)
                else:
                    this_month = str(month)
                get_day_data(this_day, this_month, str(year))
    total_time = time_converter(time.time() - start_time)
    logger.log("Done fetching " + str(year) + " pitch fx data: time = " + total_time)
    driver_logger.log("\t\tTime = " + total_time)


def get_day_data(day, month, year):
    home_page_url = 'http://gd2.mlb.com/components/game/mlb/year_' + year + '/month_' + month + '/day_' + day
    home_page = str(BeautifulSoup(urlopen(home_page_url), 'html.parser')).split('<li>')


# get_pitch_fx_data(2018, Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\"
#                                "dump.log"))