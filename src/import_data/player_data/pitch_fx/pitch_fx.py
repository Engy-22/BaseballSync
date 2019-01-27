import os
import time
import datetime
from utilities.logger import Logger
from utilities.dbconnect import DatabaseConnection
from utilities.time_converter import time_converter
from concurrent.futures import ThreadPoolExecutor
from urllib.request import urlopen, urlretrieve
from bs4 import BeautifulSoup
from xml.dom import minidom
from import_data.player_data.pitch_fx.write_to_file import write_to_file
from import_data.player_data.pitch_fx.translators.translate_outcome import translate_pitch_outcome
from import_data.player_data.pitch_fx.translators.determine_swing_take import determine_swing_or_take
from import_data.player_data.pitch_fx.translators.translate_pitch_type import translate_pitch_type
from import_data.player_data.pitch_fx.translators.determine_trajectory import determine_trajectory
from import_data.player_data.pitch_fx.translators.determine_field import determine_field
from import_data.player_data.pitch_fx.translators.determine_direction import determine_direction
from import_data.player_data.pitch_fx.translators.resolve_player_id import resolve_player_id
from import_data.player_data.pitch_fx.translators.resolve_team_id import resolve_team_id

innings = {}
strikes = 0
balls = 0
logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\logs\\import_data\\pitch_fx.log")


def get_pitch_fx_data(year, driver_logger):
    if year < 2008:
        driver_logger.log("\tNo pitch fx data to download before 2008")
        return
    driver_logger.log("\tFetching " + str(year) + " pitch fx data")
    print("Fetching " + str(year) + " pitch fx data")
    start_time = time.time()
    logger.log("Downloading pitch fx data for " + str(year) + ' || Timestamp: ' + datetime.datetime.today().
               strftime('%Y-%m-%d %H:%M:%S'))
    db = DatabaseConnection()
    opening_day = db.read('select opening_day from years where year = ' + str(year) + ';')[0][0]
    db.close()
    for month in range(3, 12, 1):
        # if month > 6:
        if month >= int(opening_day.split('-')[0]):
            for day in range(1, 32, 1):
                # if day > 29:
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
    logger.log("Done fetching " + str(year) + " pitch fx data: time = " + total_time + '\n\n\n\n')
    driver_logger.log("\t\tTime = " + total_time)


def get_day_data(day, month, year):
    logger.log("\tDownloading data for " + month + '-' + day + '-' + year)
    day_time = time.time()
    home_page_url = 'http://gd2.mlb.com/components/game/mlb/year_' + year + '/month_' + month + '/day_' + day
    a=0
    home_page = str(BeautifulSoup(urlopen(home_page_url), 'html.parser')).split('<li>')
    for line in home_page:
        try:
            if str(line.split('"day_' + str(day) + '/')[1])[:3] == 'gid':
                global innings
                innings = {}
                innings_url = home_page_url[:-6] + line.split('<a href="')[1].split('">')[0] + 'inning/'
                players_url = home_page_url[:-6] + line.split('<a href="')[1].split('">')[0] + 'players.xml'
                logger.log("\t\tDownloading data for game: " + line.split('gid_')[1].split('_')[3] + '_'
                           + line.split('gid_')[1].split('_')[4] + ' - ' + innings_url)
                print("\nDownloading data for game: " + line.split('gid_')[1].split('_')[3] + '_' + line.split('gid_')[1]
                      .split('_')[4])
                try:
                    innings_page = str(BeautifulSoup(urlopen(innings_url), 'html.parser')).split('<li>')
                    urlretrieve(players_url, 'C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\src\\'
                                             'import_data\\player_data\\pitch_fx\\xml\\players.xml')
                except Exception:
                    innings_page = []
                with ThreadPoolExecutor(os.cpu_count()) as executor:
                    for inning in innings_page:
                        try:
                            if inning.split('<a href="inning_')[1].split('.')[0].isdigit():
                                individual_inning_url = inning.split('.xml"> ')[1].split('</a>')[0]
                                # print(innings_url + individual_inning_url)
                                executor.submit(load_xml, innings_url + individual_inning_url,
                                                individual_inning_url.split('_')[1].split('.xml')[0])
                        except IndexError:
                            continue
                parse_innings(year)
                clear_xmls()
        except IndexError as e:
            a+=1
            if a==3:
                raise e
            clear_xmls()
            continue
        except KeyError as e:
            print(e)
            clear_xmls()
            continue
    logger.log("\tDone downloading data for " + month + '-' + day + '-' + year + ": time = "
               + time_converter(time.time() - day_time) + '\n\n')


def load_xml(inning_url, inning_num):
    urlretrieve(inning_url, "C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\src\\import_data\\player_data"
                            "\\pitch_fx\\xml\\" + str(inning_num) + ".xml")


def clear_xmls():
    def remove(xml_file):
        os.remove(xml_file)
    logger.log("\t\t\tClearing xml files")
    dir = "C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\src\\import_data\\player_data\\pitch_fx\\xml"
    with ThreadPoolExecutor(os.cpu_count()) as executor:
        for xml_file in os.listdir(dir):
            executor.submit(remove, dir + '\\' + xml_file)


def parse_innings(year):
    dir = "C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\src\\import_data\\player_data\\pitch_fx\\xml"
    doc = minidom.parse(dir + '\\players.xml')
    away_team = resolve_team_id(doc.getElementsByTagName('team')[0].getAttribute('id'))
    home_team = resolve_team_id(doc.getElementsByTagName('team')[1].getAttribute('id'))
    print(home_team)
    print(away_team)
    for xml_file in os.listdir(dir):
        if 'players' not in xml_file:
            parse_inning(year, dir + '\\' + xml_file, away_team, home_team)


def parse_inning(year, xml_file, away_team, home_team):
    # print(xml_file)
    doc = minidom.parse(xml_file)
    away_at_bats = doc.getElementsByTagName('inning')[0].getElementsByTagName('top')[0].getElementsByTagName('atbat')
    for at_bat in away_at_bats:
        parse_at_bat(year, at_bat, home_team, away_team)
    try:
        home_at_bats = doc.getElementsByTagName('inning')[0].getElementsByTagName('bottom')[0]\
            .getElementsByTagName('atbat')
        for at_bat in home_at_bats:
            parse_at_bat(year, at_bat, away_team, home_team)
    except IndexError:
        pass


def parse_at_bat(year, at_bat, pitcher_team, hitter_team):
    meta_data = {'original_pitcher_id': at_bat.getAttribute('pitcher'),
                 'original_batter_id': at_bat.getAttribute('batter'),
                 'pitcher_id': resolve_player_id(at_bat.getAttribute('pitcher'), year, pitcher_team, 'pitching'),
                 'pitcher_team': pitcher_team,
                 'batter_id': resolve_player_id(at_bat.getAttribute('batter'), year, hitter_team, 'batting'),
                 'batter_team': hitter_team,
                 'temp_outcome': at_bat.getAttribute('event'),
                 'ab_description': at_bat.getAttribute('des'),
                 'batter_orientation': 'v' + at_bat.getAttribute('stand="').lower() + 'hb',
                 'pitcher_orientation': 'v' + at_bat.getAttribute('p_throws="').lower() + 'hp'}
    pitches = at_bat.getElementsByTagName('pitch')
    global strikes
    strikes = 0
    global balls
    balls = 0
    for pitch in pitches:
        parse_pitch(year, pitch, meta_data, pitches.index(pitch)+1 == len(pitches))
    actions = at_bat.getElementsByTagName('action')
    if len(actions) > 0:
        print(actions)


def parse_pitch(year, pitch, meta_data, last_pitch):
    global strikes
    global balls
    count = str(balls) + '-' + str(strikes)
    if pitch.getAttribute('type') == "B":
        ball_strike = "ball"
        balls += 1
    else:
        ball_strike = "strike"
        if strikes < 2:
            strikes += 1
    if last_pitch:
        outcome = translate_pitch_outcome(meta_data['temp_outcome'], meta_data['ab_description'])
        trajectory = determine_trajectory(outcome, meta_data['ab_description'])
        field = determine_field(outcome)
        direction = determine_direction(meta_data['ab_description'], meta_data['batter_orientation'])
    else:
        outcome = "none"
        trajectory = "none"
        field = "none"
        direction = "none"
    try:
        pitch_type = translate_pitch_type(pitch.getAttribute('pitch_type'))
        swing_take = determine_swing_or_take(pitch.getAttribute('des'))
        with ThreadPoolExecutor(os.cpu_count()) as executor2:
            executor2.submit(write_to_file, 'pitcher', meta_data['pitcher_id'], meta_data['pitcher_team'], year,
                             meta_data['batter_orientation'], count, pitch_type, ball_strike, swing_take, outcome,
                             trajectory, field, direction, meta_data['original_pitcher_id'])
            executor2.submit(write_to_file, 'batter', meta_data['batter_id'], meta_data['batter_team'], year,
                             meta_data['pitcher_orientation'], count, pitch_type, ball_strike, swing_take, outcome,
                             trajectory, field, direction, meta_data['original_batter_id'])
    except KeyError:
        pass


for year in range(2018, 2019, 1):
    get_pitch_fx_data(year, Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\"
                                   "dump.log"))
# get_day_data('10', '05', '2018')
