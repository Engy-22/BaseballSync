import os
import time
import datetime
from bs4 import BeautifulSoup
from urllib.request import urlopen, urlretrieve
from xml.dom import minidom
from import_data.player_data.pitch_fx.translators.translate_pitch_type import translate_pitch_type
from import_data.player_data.pitch_fx.translators.resolve_player_id import resolve_player_id
from import_data.player_data.pitch_fx.translators.resolve_team_id import resolve_team_id
from utilities.properties import simulsync_driver_logger
from utilities.time_converter import time_converter
from concurrent.futures import  ThreadPoolExecutor


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
    simulsync_driver_logger.log('\tTime = ' + time_converter(time.time() - start_time))
    return [home_page_url + url for url in home_page]


def fetch_game(url, past_game_data, key):
    year = key.split('year_')[1][0:4]
    scoreboard = str(BeautifulSoup(urlopen(url + 'miniscoreboard.xml'), 'html.parser'))
    game_status = scoreboard.split('<game_status ')[1].split('status=')[1].split('"')[1]
    if game_status in ['Final', 'Game Over']:
        return 'completed', False, None, None, None, None, None, None, None, None, None, None
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
            urlretrieve(url + 'players.xml', os.path.join("..", "..", "baseball-sync", "src", "import_data",
                                                          "player_data", "pitch_fx", "xml", "players.xml"))
        except Exception:
            return 'in progress', False, 'get_the_latest_pitch', None, None, None, None, None, None, None, None, None
    else:
        return game_status, False, None, None, None, None, None, None, None, None, None, None
    new_pitch, latest_pitch_id, pitch_description, pitch_type, pitch_outcome, x, y, velocity, batter, pitcher,\
    outcome = get_latest_pitch(past_game_data, year, key)
    return 'in progress', new_pitch, latest_pitch_id, pitch_description, translate_pitch_type(pitch_type),\
           pitch_outcome, x, y, velocity, batter, pitcher, outcome


def get_latest_pitch(past_game_data, year, key):
    pitch_outcomes = {'B': 'ball', 'S': 'strike', 'X': 'in play'}
    inning = ['top', 'bottom']
    try:
        simulsync_xml = minidom.parse(os.path.join('simulsync', 'simulsync.xml'))
        inning_info = simulsync_xml.getElementsByTagName('inning')[0]
        away_team = resolve_team_id(inning_info.getAttribute('away_team'))
        home_team = resolve_team_id(inning_info.getAttribute('home_team'))
        player_teams = {}
        if past_game_data[key]['latest_pitch'] != 'get_the_latest_pitch':
            latest_pitch_found = False
            new_pitch = False
            latest_pitch = past_game_data[key]['latest_pitch']
            pitch_description = pitch_type = pitch_outcome = x_coord = y_coord = velocity = batter = pitcher =\
                at_bat_outcome = None
            for top_bottom in inning:
                if top_bottom == 'top':
                    player_teams['batter'] = away_team
                    player_teams['pitcher'] = home_team
                else:
                    player_teams['batter'] = home_team
                    player_teams['pitcher'] = away_team
                for at_bat in simulsync_xml.getElementsByTagName(top_bottom)[0].getElementsByTagName('atbat'):
                    batter = at_bat.getAttribute('batter')
                    pitcher = at_bat.getAttribute('pitcher')
                    at_bat_outcome = at_bat.getAttribute('des')
                    for pitch in at_bat.getElementsByTagName('pitch'):
                        if latest_pitch_found:
                            new_pitch = True
                            latest_pitch = pitch.getAttribute('play_guid')
                            pitch_description = pitch.getAttribute('des')
                            pitch_type = pitch.getAttribute('pitch_type')
                            pitch_outcome = pitch_outcomes[pitch.getAttribute('type')]
                            x_coord = pitch.getAttribute('x')
                            y_coord = pitch.getAttribute('y')
                            velocity = pitch.getAttribute('start_speed')
                        else:
                            if pitch.getAttribute('play_guid') == past_game_data[key]['latest_pitch']:
                                latest_pitch_found = True
                return new_pitch, latest_pitch, pitch_description, pitch_type, pitch_outcome, x_coord, y_coord,\
                       velocity, resolve_player_id(batter, year, player_teams['batter'], 'batting'),\
                       resolve_player_id(pitcher, year, player_teams['pitcher'], 'pitching'), at_bat_outcome
        else:
            bottom_of_inning = simulsync_xml.getElementsByTagName('bottom')[0]
            if len(bottom_of_inning.getElementsByTagName('atbat')) > 0:
                batter_team = home_team
                pitcher_team = away_team
            else:
                batter_team = away_team
                pitcher_team = home_team
            latest_at_bat = simulsync_xml.getElementsByTagName('atbat')[-1]
            pitch = latest_at_bat.getElementsByTagName('pitch')[-1]
            return True, pitch.getAttribute('play_guid'), pitch.getAttribute('des'), pitch.getAttribute('pitch_type'),\
                   pitch_outcomes[pitch.getAttribute('type')], pitch.getAttribute('x'), pitch.getAttribute('y'),\
                   pitch.getAttribute('start_speed'),\
                   resolve_player_id(latest_at_bat.getAttribute('batter'), year, batter_team, 'batting'),\
                   resolve_player_id(latest_at_bat.getAttribute('pitcher'), year, pitcher_team, 'pitching'),\
                   latest_at_bat.getAttribute('des')
    except IndexError:
        return False, past_game_data[key]['latest_pitch'], None, None, None, None, None, None, None, None, None
