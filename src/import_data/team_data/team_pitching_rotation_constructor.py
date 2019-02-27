import os
import time
import datetime
from urllib.request import urlopen
from bs4 import BeautifulSoup
from utilities.database.wrappers.baseball_data_connection import DatabaseConnection
from utilities.translate_team_id import translate_team_id
from utilities.logger import Logger
from utilities.time_converter import time_converter
from concurrent.futures import ThreadPoolExecutor
from utilities.properties import sandbox_mode, import_driver_logger as driver_logger

pages = {}
logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\"
                "team_pitching_rotation_constructor.log")


def team_pitching_rotation_constructor(year):
    if year < 1908:
        logger.log("No team pitching rotation data to download. (before 1908).")
        driver_logger.log("\tNo team pitching rotation data to download. (before 1908).")
        return
    print("getting team schedules and pitching rotations")
    driver_logger.log("\tGetting team schedules and pitching rotations")
    start_time = time.time()
    global pages
    pages = {}
    logger.log("Downloading " + str(year) + " team batting order data || Timestamp: " + datetime.datetime.today()
               .strftime('%Y-%m-%d %H:%M:%S'))
    logger.log("\tdownloading team pages")
    with open("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\background\\yearTeams.txt", 'r') as year_file:
        with ThreadPoolExecutor(os.cpu_count()) as executor:
            for line in year_file:
                if str(year) in line:
                    temp_line = line.split(',')[1:-1]
                    for team in temp_line:
                        if "TOT" not in team:
                            executor.submit(load_url, year, team.split(';')[0], team.split(';')[1])
                    break
    logger.log("\t\tTime = " + time_converter(time.time() - start_time))
    logger.log("\tOrganizing schedules and pitching rotations")
    write_time = time.time()
    get_pitchers(year)
    logger.log("\t\t\tTime = " + time_converter(time.time() - write_time))
    total_time = time_converter(time.time() - start_time)
    logger.log("Done downloading team pitching rotation data: time = " + total_time + '\n\n')
    driver_logger.log("\t\tTime = " + total_time)


def load_url(year, team_id, team_key):
    logger.log("\t\tdownloading " + team_id + " page")
    pages[team_id] = str(BeautifulSoup(urlopen("https://www.baseball-reference.com/teams/" + team_key + "/"
                                               + str(year) + "-lineups.shtml"), "html.parser")).\
        split('grid_table sortable')[1].split('tbody')[1].split('<tr')


def get_pitchers(year):
    global pages
    for team_id, def_lineups in pages.items():
        logger.log("\t\tGathering " + team_id + " schedule and pitching rotations")
        pitcher_list = []
        schedule = {}
        try:
            for i in def_lineups:
                if 'th class="left "' in i:
                    pitcher_list.append(i.split('data-stat="P"><a data-entry-id="')[1].split('"')[0].replace("'", "\'"))
                    game_num = i.split('<a name="')[1].split('"')[0]
                    schedule[game_num] = []
                    schedule[game_num].append(translate_team_id(i.split('<a href="/teams/')[1].split('/')[0], year))
                    schedule[game_num].append(i.split('">' + schedule[game_num][0] + '</a> ')[1].split(' ')[0])
                    schedule[game_num].append(i.split(schedule[game_num][0] + '</a> ' + schedule[game_num][1]
                                                      + ' (')[1].split(')')[0])
                    date = i.split('title="facing:')[1].split('">')[1].split(',')[1].split('</a>')[0].split('/')
                    schedule[game_num].append(date[0])
                    schedule[game_num].append(date[1])
            with ThreadPoolExecutor(os.cpu_count()) as executor2:
                executor2.submit(write_to_file_pitchers, team_id, year, pitcher_list)
                executor2.submit(write_to_file_schedule, team_id, year, schedule)
        except IndexError:
            pass


def write_to_file_pitchers(team, year, pitchers):
    db = DatabaseConnection(sandbox_mode)
    for i in range(len(pitchers)):
        if len(db.read('select starterId from starting_pitchers where playerId = "' + pitchers[i] + '" and gameNum = '
                       + str(i+1) + ' and TY_uniqueidentifier = (select TY_uniqueidentifier from team_years where '
                       'teamId = "' + team + '" and year = ' + str(year) + ');')) == 0:
            db.write('insert into starting_pitchers (starterId, playerId, gameNum, TY_uniqueidentifier) values (default'
                     ', "' + pitchers[i] + '", ' + str(i+1) + ', (select TY_uniqueidentifier from team_years where '
                     'teamId = "' + team + '" and year = ' + str(year) + '));')
    db.close()


def write_to_file_schedule(team_id, year, schedule):
    db = DatabaseConnection(sandbox_mode)
    ty_uid = db.read('select ty_uniqueidentifier from team_years where teamid = "' + team_id + '" and year = '
                     + str(year) + ';')[0][0]
    for game, data in schedule.items():
        if len(db.read('select gameid from schedule where ty_uniqueidentifier = ' + str(ty_uid) + ' and game_num = '
                       + game + ';')) == 0:
            db.write('insert into schedule (gameid, day, month, year, ty_uniqueidentifier, game_num, opponent, outcome,'
                     ' score) values (default, ' + str(data[4]) + ', ' + str(data[3]) + ', ' + str(year) + ', '
                     + str(ty_uid) + ', ' + game + ', (select ty_uniqueidentifier from team_years where teamid = "'
                     + data[0] + '" and year = ' + str(year) + '), "' + data[1] + '", "' + data[2] + '");')
    db.close()


# dump_logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\dump.log")
# for year in range(1996, 1998, 1):
# team_pitching_rotation_constructor(2012, dump_logger)
