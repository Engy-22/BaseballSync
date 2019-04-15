import os
import time
import datetime
from bs4 import BeautifulSoup
from urllib.request import urlopen, urlretrieve
from xml.dom import minidom
from concurrent.futures import ThreadPoolExecutor
from utilities.properties import simulsync_driver_logger
from utilities.time_converter import time_converter


def fetch_home_pages():
    start_time = time.time()
    date = datetime.datetime.today()
    simulsync_driver_logger.log('Fetching home page for ' + datetime.datetime.today().strftime('%Y-%m-%d'))
    day = str(date.day)
    month = str(date.month)
    if len(str(day)) == 1:
        day = '0' + day
    if len(str(month)) == 1:
        month = '0' + month
    home_page_url = 'http://gd2.mlb.com/components/game/mlb/year_' + str(date.year) + '/month_' + month + '/day_' + day
    # home_page_url = 'http://gd2.mlb.com/components/game/mlb/year_2018/month_05/day_07'
    home_page = str(BeautifulSoup(urlopen(home_page_url), 'html.parser')).split('<li>')
    simulsync_driver_logger.log('\tTime = ' + time_converter(time.time()-start_time))
    return [home_page_url + url for url in home_page]


def fetch_game(url, past_game_data):
    scoreboard = str(BeautifulSoup(urlopen(url + 'miniscoreboard.xml'), 'html.parser'))
    game_status = scoreboard.split('<game_status ')[1].split('status=')[1].split('"')[1]
    if game_status in ['Final', 'Game Over']:
        return 'completed', None
    elif game_status == "In Progress":
        game_page = str(BeautifulSoup(urlopen(url + 'inning/'), 'html.parser')).split('<li>')
        innings = []
        for inning in game_page:
            try:
                if inning.split('<a href="inning_')[1].split('.')[0].isdigit():
                    innings.append(inning.split('<a href=')[1].split('"')[1])
            except IndexError:
                continue
        try:
            urlretrieve(url + 'inning/' + innings[-1], os.path.join('simulsync', 'simulsync.xml'))
        except Exception:
            return 'in progress', 'get_the_latest_pitch'
    else:
        return game_status, None
    return 'in progress', get_latest_pitch(past_game_data)


def get_latest_pitch(past_game_data):
    try:
        simulsync_xml = minidom.parse(os.path.join('simulsync', 'simulsync.xml'))
        if past_game_data['latest_pitch'] != 'get_the_latest_pitch':
            latest_pitch_found = False
            latest_pitch = past_game_data['latest_pitch']
            for at_bat in simulsync_xml.getElementsByTagName('atbat'):
                for pitch in at_bat.getElementsByTagName('pitch'):
                    if latest_pitch_found:
                        latest_pitch = pitch.getAttribute('play_guid')
                        print('\t' + pitch.getAttribute('pitch_type'))
                    else:
                        if pitch.getAttribute('play_guid') == past_game_data['latest_pitch']:
                            latest_pitch_found = True
            return latest_pitch
        else:
            latest_at_bat = simulsync_xml.getElementsByTagName('atbat')[-1]
            print('\t' + latest_at_bat.getElementsByTagName('pitch')[-1].getAttribute('pitch_type'))
            return latest_at_bat.getElementsByTagName('pitch')[-1].getAttribute('play_guid')
    except IndexError:
        return past_game_data['latest_pitch']
