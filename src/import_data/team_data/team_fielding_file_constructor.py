import os
import time
import datetime
from urllib.request import urlopen
from bs4 import BeautifulSoup
from utilities.database.wrappers.baseball_data_connection import DatabaseConnection
from utilities.time_converter import time_converter
from utilities.logger import Logger
from concurrent.futures import ThreadPoolExecutor
from utilities.properties import sandbox_mode, log_prefix, import_driver_logger as driver_logger

pages = {}
logger = Logger(os.path.join(log_prefix, "import_data", "team_fielding_file_constructor.log"))


def team_fielding_file_constructor(year):
    print('getting team fielding positions')
    driver_logger.log("\tGetting team fielding positions")
    start_time = time.time()
    global pages
    pages = {}
    logger.log("Downloading " + str(year) + " team fielding positions || Timestamp: " + datetime.datetime.today()
               .strftime('%Y-%m-%d %H:%M:%S'))
    logger.log("\tDownloading team pages")
    try:
        year_file = open(os.path.join("..", "background", "yearTeams.txt"), 'r')
    except FileNotFoundError:
        year_file = open(os.path.join("..", "..", "..", "background", "yearTeams.txt"), 'r')
    with ThreadPoolExecutor(os.cpu_count()) as executor:
        for line in year_file:
            if str(year) in line:
                temp_line = line.split(',')[1:-1]
                for team in temp_line:
                    split_team = team.split(';')
                    if "TOT" not in split_team:
                        executor.submit(load_url, year, split_team[0], split_team[1])
                year_file.close()
                break
    logger.log("\t\tTime = " + time_converter(time.time() - start_time))
    logger.log("\tOrganizing team position data")
    write_time = time.time()
    write_to_file(year)
    logger.log("\t\tTime = " + time_converter(time.time() - write_time))
    total_time = time_converter(time.time() - start_time)
    logger.log("Done downloading team fielding data: time = " + total_time + '\n\n')
    driver_logger.log("\t\tTime = " + total_time)


def load_url(year, team_id, team_key):
    logger.log("\t\tdownloading " + team_id + " page")
    pages[team_id] = str(BeautifulSoup(urlopen("https://www.baseball-reference.com/teams/" + team_key + "/" + str(year)
                                               + "-fielding.shtml"), "html.parser")).split('<tbody>')[1]\
        .split('</tbody>')[0].split('<tr')


def write_to_file(year):
    for team_id, table in pages.items():
        logger.log("\t\tgathering and writing " + team_id + " positions")
        db = DatabaseConnection(sandbox_mode)
        primary_keys = []
        try:
            for row in table:
                if 'data-append-csv="' in row:
                    this_string = ""
                    primary_keys.append(row.split('data-append-csv="')[1].split('"')[0].replace("'", "\'"))
                    this_string += '"' + primary_keys[-1] + '","'
                    position_summary = row.split('data-stat="pos_summary" >')[1].split('<')[0]
                    if '-' in position_summary:
                        positions = position_summary.split('-')
                        for position_index in range(len(positions)):
                            if this_is_position_player_pitching(primary_keys[-1], positions, position_index, team_id,
                                                                year) or \
                                    this_is_pitcher_playing_in_the_field(primary_keys[-1], positions, position_index,
                                                                         team_id, year):
                                continue  # don't give positions players RP eligibility who threw mop-up innings
                            else:
                                if position_index != len(positions)-1:
                                    this_string += positions[position_index] + ","
                                else:
                                    this_string += positions[position_index]
                    else:
                        this_string += position_summary
                    this_string += '"'
                    ty_uid = str(db.read('select TY_uniqueidentifier from team_years where teamId = "' + team_id
                                         + '" and year = ' + str(year) + ';')[0][0])
                    if len(db.read('select PPos_uniqueidentifier from player_positions where playerId='
                                   + this_string.split(',')[0] + ' and TY_uniqueidentifier = ' + ty_uid + ';')) == 0:
                        db.write('insert into player_positions (PPos_uniqueidentifier, playerId, positions, '
                                 'TY_uniqueidentifier) values (default, ' + this_string + ', ' + ty_uid + ');')
                    else:
                        split_positions = this_string.split(',')[1:]
                        if split_positions[-1] == '"':
                            del split_positions[-1]
                            split_positions[-1] += '"'
                        db.write('update player_positions set positions = ' + ','.join(split_positions) + ' where '
                                 'playerId = ' + this_string.split(',')[0] + ' and TY_uniqueidentifier = ' + ty_uid
                                 + ';')
        except IndexError:
            pass
        db.close()


def this_is_position_player_pitching(player_id, position_list, position_index, team_id, year):
    this_is_a_position_player_pitching = False
    if position_list[position_index] == 'P' and \
            any(position in position_list for position in ['C', '1B', '2B', '3B', 'SS', 'LF', 'CF', 'RF', 'DH']):
        db = DatabaseConnection(sandbox_mode=True)
        if float(db.read('select ip from player_pitching where year = ' + str(year) + ' and pt_uniqueidentifier = '
                         '(select pt_uniqueidentifier from player_teams where playerId = "' + player_id + '" and teamId'
                         ' = "' + team_id + '");')[0][0]) < 10.0:
            this_is_a_position_player_pitching = True
        db.close()
    return this_is_a_position_player_pitching


def this_is_pitcher_playing_in_the_field(player_id, position_list, position_index, team_id, year):
    this_is_a_position_player_pitching = False
    if position_list[position_index] != 'P' and 'P' in position_list:
        db = DatabaseConnection(sandbox_mode=True)
        where_clause = 'where year = ' + str(year) + ' and pt_uniqueidentifier = (select pt_uniqueidentifier from' \
                       ' player_teams where playerId = "' + player_id + '" and teamId = "' + team_id + '");'
        if float(db.read('select ip from player_pitching ' + where_clause)[0][0]) >= 10.0:
            if int(db.read('select pa from player_batting ' + where_clause)[0][0]) < 25:
                this_is_a_position_player_pitching = True
        db.close()
    return this_is_a_position_player_pitching


# team_fielding_file_constructor(2017)
