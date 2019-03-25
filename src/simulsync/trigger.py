import os
import datetime
from bs4 import BeautifulSoup
from urllib.request import urlopen
from xml.dom import minidom
from concurrent.futures import ThreadPoolExecutor
from utilities.properties import simulsync_driver_logger


def fetch_pages():
    date = datetime.datetime.today()
    home_page_url = 'http://gd2.mlb.com/components/game/mlb/year_' + str(date.year) + '/month_' + str(date.month)\
                    + '/day_' + str(date.day)
    home_page = str(BeautifulSoup(urlopen(home_page_url), 'html.parser')).split('<li>')
    return home_page
