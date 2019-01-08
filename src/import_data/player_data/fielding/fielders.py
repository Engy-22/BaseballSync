import os
import time
import datetime
from urllib.request import urlopen, urlretrieve
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from utilities.DB_Connect import DB_Connect
from utilities.translate_team_id import translate_team_id
from utilities.time_converter import time_converter
from utilities.Logger import Logger

data = {}

logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\fielders.log")


def fielding_constructor(year, driver_logger):
    print('Downloading fielder images and attributes')
    driver_logger.log("\tDownloading fielder images and attributes")
    start_time = time.time()
    logger.log("Downloading fielder " + str(year) + " data || Timestamp: " + datetime.datetime.today()\
               .strftime('%Y-%m-%d %H:%M:%S'))
    logger.log("\tAssembling list of players")
    table = str(BeautifulSoup(urlopen("https://www.baseball-reference.com/leagues/MLB/" + str(year)
                                      + "-standard-fielding.shtml"), 'html.parser')).\
        split('<table class="sortable stats_table" id')[1].split('<tbody>')[1].split('</tbody>')[0].split('<tr')
    count = 0
    for row in table:
        if 'data-stat="player" csk="' in row and 'data-append-csv="' in row:
            data[str(count)] = {'player_id': row.split('data-append-csv="')[1].split('"')[0].replace("'", "\'"),
                                'row': row.split('data-stat'),
                                'temp_player': row.split('data-stat="player" csk="')[1].split('" >')[0]}
            count += 1
    logger.log("\t\tDone assembling list of players")
    bulk_time = time.time()
    logger.log("\tParsing player pages, downloading images, and extracting player attributes")
    with ThreadPoolExecutor(os.cpu_count()) as executor:
        for id_num, dictionary in data.items():
            executor.submit(intermediate, id_num, year, dictionary['player_id'], dictionary['temp_player'],
                            dictionary['row'])
    for id_num, dictionary in data.items():
        write_teams_and_stats(dictionary['player_id'], dictionary['stats'], dictionary['team'], year)
    logger.log("\t\tTime = " + time_converter(time.time() - bulk_time))
    total_time = time_converter(time.time() - start_time)
    logger.log("Done downloading player images and attributes: time = " + total_time + '\n\n')
    driver_logger.log("\t\tTime = " + total_time)


def extract_player_attributes(player_id, page, reversed_name):
    urlretrieve(str(page.find_all('img')[1]).split('src=')[1].split('/>')[0].split('"')[1],
                "C:\\Users\\Anthony Raimondo\\images\\players\\" + player_id + ".jpg")
    for ent in page.find_all('div'):
        str_ent = str(ent)
        if 'Throws: </strong>' in str_ent:
            return {'lastName': reversed_name.split(',')[1], 'firstName': reversed_name.split(',')[0],
                    'throwsWith': str_ent.split('Bats: </strong>')[1][0], 'primaryPosition': 'N',
                    'batsWith': str_ent.split('Throws: </strong>')[1][0]}


def get_team_and_stats(id_num, row, year):
    stats = {"CH": "chances", "PO": "PO", "A": "A", "E": "E_def", "team_ID": "team_ID"}
    stat_dictionary = {}
    team = "TOT"
    for ent in row:
        for stat, name in stats.items():
            if '="' + name + '" >' in ent:
                try:
                    if name == "team_ID":
                        try:
                            team = translate_team_id(ent.split('a href="/teams/')[1].split('/')[0], year)
                        except IndexError:
                            pass
                    else:
                        stat_dictionary[stat] = int(ent.split('="' + name + '" >')[1].split('<')[0])
                except ValueError:
                    pass
                break
    data[id_num]['stats'] = stat_dictionary
    data[id_num]['team'] = team


def load_url(player_id):
    page = None
    db, cursor = DB_Connect.grab("baseballData")
    if len(DB_Connect.read(cursor, 'select * from players where playerid = "' + player_id + '";')) == 0:
        page = BeautifulSoup(urlopen("https://www.baseball-reference.com/players/" + player_id[0] + "/" + player_id
                                     + ".shtml"), 'html.parser')
    DB_Connect.close(db)
    return page


def write_to_db(player_id, player_attributes):
    fields = ''
    values = ''
    for field, value in player_attributes.items():
        fields += ', ' + field
        values += '", "' + value
    db, cursor = DB_Connect.grab("baseballData")
    if len(DB_Connect.read(cursor, 'select * from players where playerid = "' + player_id + '";')) == 0:
        DB_Connect.write(db, cursor, 'insert into players (playerid ' + fields + ') values ("' + player_id + values
                                     + '");')
    DB_Connect.close(db)


def write_teams_and_stats(player_id, stats, team, year):
    db, cursor = DB_Connect.grab("baseballData")
    if len(DB_Connect.read(cursor, 'select pt_uniqueidentifier from player_teams where playerid = "' + player_id
                                   + '" and teamid = "' + team + '";')) == 0:
        DB_Connect.write(db, cursor, 'insert into player_teams (pt_uniqueidentifier, playerid, teamid) values (default,'
                                     ' "' + player_id + '", "' + team + '");')
    if len(DB_Connect.read(cursor, 'select pf_uniqueidentifier from player_fielding where year = ' + str(year)
                                   + ' and pt_uniqueidentifier = (select pt_uniqueidentifier from player_teams where '
                                   'playerid = "' + player_id + '" and teamid = "' + team + '");')) == 0:
        fields = ''
        values = ''
        for field, value in stats.items():
            fields += ', ' + field
            values += ', ' + str(value)
        DB_Connect.write(db, cursor, 'insert into player_fielding (pf_uniqueidentifier, year, pt_uniqueidentifier, '
                                     'complete_year' + fields + ') values (default, ' + str(year) + ', (select '
                                     'pt_uniqueidentifier from player_teams where playerid = "' + player_id + '" and '
                                     'teamid = "' + team + '"), FALSE' + values + ');')
    DB_Connect.close(db)


def intermediate(id_num, year, player_id, temp_player, row):
    page = load_url(player_id)
    if page is not None:
        if "-0" in temp_player:
            reversed_name = temp_player.split("-0")[0].replace("'", "\'")
        else:
            reversed_name = temp_player.split("0")[0].replace("'", "\'")
        write_to_db(player_id, extract_player_attributes(player_id, page, reversed_name))
    get_team_and_stats(id_num, row, year)


# fielding_constructor(2018, Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\"
#                                  "dump.log"))
