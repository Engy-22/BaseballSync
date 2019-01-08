import os
import time
import datetime
from urllib.request import urlopen
from bs4 import BeautifulSoup
from utilities.DB_Connect import DB_Connect
from utilities.time_converter import time_converter
from utilities.Logger import Logger
from concurrent.futures import ThreadPoolExecutor

logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\"
                "team_fielding_file_constructor.log")


def team_fielding_file_constructor(year, driver_logger):
    print('getting team fielding positions')
    driver_logger.log("\tgetting team fielding positions")
    start_time = time.time()
    logger.log("Downloading " + str(year) + " team fielding positions || Timestamp: " + datetime.datetime.today()\
               .strftime('%Y-%m-%d %H:%M:%S'))
    with open("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\background\\yearTeams.txt", 'r') as year_file:
        with ThreadPoolExecutor(os.cpu_count()) as executor:
            for line in year_file:
                if str(year) in line:
                    temp_line = line.split(',')[1:-1]
                    for team in temp_line:
                        split_team = team.split(';')
                        if "TOT" not in split_team:
                            executor.submit(write_to_file, split_team[0], split_team[1], year)
                    break
    total_time = time_converter(time.time() - start_time)
    logger.log("Done downloading team fielding data: time = " + total_time + '\n\n')
    driver_logger.log("\t\tTime = " + total_time)


def write_to_file(team, team_key, year):
    logger.log("\tgathering and writing " + team + " positions")
    db, cursor = DB_Connect.grab("baseballData")
    primary_keys = []
    try:
        table = str(BeautifulSoup(urlopen("https://www.baseball-reference.com/teams/" + team_key + "/" + str(year) + "-"
                                          "fielding.shtml"), "html.parser")).split('<tbody>')[1].split('</tbody>')[0]\
            .split('<tr')
        for row in table:
            if 'data-append-csv="' in row:
                this_string = ""
                primary_keys.append(row.split('data-append-csv="')[1].split('"')[0].replace("'", "\'"))
                this_string += '"' + primary_keys[-1] + '","'
                position_summary = row.split('data-stat="pos_summary" >')[1].split('<')[0]
                if '-' in position_summary:
                    positions = position_summary.split('-')
                    for i in range(len(positions)):
                        if i != len(positions)-1:
                            this_string += positions[i] + ","
                        else:
                            this_string += positions[i]
                else:
                    this_string += position_summary
                this_string += '"'
                if len(DB_Connect.read(cursor, 'select PPos_uniqueidentifier from player_positions where playerId = '
                                               + this_string.split(',')[0] + ' and TY_uniqueidentifier = (select '
                                               + 'TY_uniqueidentifier from team_years where teamId = "' + team
                                               + '" and year = ' + str(year) + ');')) == 0:
                    DB_Connect.write(db, cursor, 'insert into player_positions (PPos_uniqueidentifier, playerId, '
                                                 'positions, TY_uniqueidentifier) values (default, ' + this_string
                                                 + ', (select TY_uniqueidentifier from team_years where teamId = "'
                                                 + team + '" and year = ' + str(year) + '));')
    except IndexError:
        pass
    DB_Connect.close(db)


# team_fielding_file_constructor(2018, Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\"
#                                             "import_data\\dump.log"))
