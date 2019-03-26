import os
import time
import datetime
from bs4 import BeautifulSoup
from urllib.request import urlopen
from xml.dom import minidom
from concurrent.futures import ThreadPoolExecutor
from utilities.properties import simulsync_driver_logger
from utilities.time_converter import time_converter


def fetch_home_pages():
    start_time = time.time()
    date = datetime.datetime.today()
    simulsync_driver_logger.log('Fetching home page for ' + datetime.datetime.today().strftime('%Y-%m-%d'))
    # home_page_url = 'http://gd2.mlb.com/components/game/mlb/year_' + str(date.year) + '/month_' + str(date.month)\
    #                 + '/day_' + str(date.day)
    home_page_url = 'http://gd2.mlb.com/components/game/mlb/year_2018/month_05/day_07'
    home_page = str(BeautifulSoup(urlopen(home_page_url), 'html.parser')).split('<li>')
    simulsync_driver_logger.log('\tTime = ' + time_converter(start_time-time.time()))
    return [home_page_url + url for url in home_page]


def fetch_game(url):
    game_page = str(BeautifulSoup(urlopen(url), 'html.parser'))
    return game_page
