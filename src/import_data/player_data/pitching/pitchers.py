import os
import time
import datetime
from urllib.request import urlopen, urlretrieve
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from utilities.database.wrappers.baseball_data_connection import DatabaseConnection
from import_data.player_data.pitching.league_pitching_ratios_constructor import league_pitching_ratios_constructor
from utilities.translate_team_id import translate_team_id
from utilities.time_converter import time_converter
from utilities.logger import Logger
from utilities.anomaly_team import anomaly_team
from utilities.properties import sandbox_mode, import_driver_logger as driver_logger

data = {}
logger = Logger(os.path.join("..", "..", "baseball-sync", "logs", "import_data", "pitchers.log"))


def pitching_constructor(year):
    global data
    data = {}
    print('Downloading pitcher images and attributes')
    driver_logger.log("\tDownloading pitcher images and attributes")
    start_time = time.time()
    logger.log("Downloading pitcher " + str(year) + " data || Timestamp: " + datetime.datetime.today()\
               .strftime('%Y-%m-%d %H:%M:%S'))
    logger.log("\tAssembling list of players")
    table = str(BeautifulSoup(urlopen("https://www.baseball-reference.com/leagues/MLB/" + str(year)
                                      + "-standard-pitching.shtml"), 'html.parser')).\
        split('<table class="sortable stats_table" id')[1].split('<tbody>')[1].split('</tbody>')[0].split('<tr')
    for row in table:
        if 'data-stat="player" csk="' in row and 'data-append-csv="' in row:
            player_id = row.split('data-append-csv="')[1].split('"')[0].replace("'", "\'")
            try:
                team = translate_team_id(row.split('a href="/teams/')[1].split('/')[0], year)
                if len(team) == 4:
                    team = anomaly_team(year)
            except IndexError:
                team = 'TOT'
            if player_id not in data:
                data[player_id] = {}
            if team not in data[player_id]:
                this_index = 1
                data[player_id][team] = {}
            else:
                if team != 'TOT':
                    this_index = len(data[player_id][team]) + 1
                else:
                    continue
            data[player_id][team][this_index] = {'row': row.split('data-stat'),
                                                 'temp_player': row.split('ata-stat="player" csk="')[1].split('" >')[0]}
    try:
        table2 = str(BeautifulSoup(urlopen("https://www.baseball-reference.com/leagues/MLB/" + str(year) + "-batting-"
                                           "pitching.shtml"), 'html.parser')).\
            split('<h2>Player Batting Against</h2>')[1].split('</tbody>')[0].split('<tbody>')[1].split('<tr ')
    except Exception:
        table2 = []
    batting_against_rows = {}
    for row in table2:
        if 'data-stat="player" csk="' in row and 'data-append-csv="' in row:
            try:
                team = translate_team_id(row.split('a href="/teams/')[1].split('/')[0], year)
            except IndexError:
                team = 'TOT'
            player_id = row.split('data-append-csv="')[1].split('"')[0].replace("'", "\'")
            if player_id not in batting_against_rows:
                batting_against_rows[player_id] = {}
            if team not in batting_against_rows[player_id]:
                this_index = 1
                batting_against_rows[player_id][team] = {}
            else:
                if team != 'TOT':
                    this_index = len(batting_against_rows[player_id][team]) + 1
                else:
                    continue
            batting_against_rows[player_id][team][this_index] = {'row': row.split('data-stat'), 'temp_player': row.
                split('ata-stat="player" csk="')[1].split('" >')[0]}
    logger.log("\t\tDone assembling list of players")
    bulk_time = time.time()
    ratios = league_pitching_ratios_constructor(year, logger)
    logger.log("\tParsing player pages, downloading images, and extracting player attributes")
    with ThreadPoolExecutor(os.cpu_count()) as executor:
        for player_id, dictionary in data.items():
            for team, dictionary2 in dictionary.items():
                for index, dictionary3 in dictionary2.items():
                    try:
                        executor.submit(intermediate, player_id, team, index, dictionary3['temp_player'],
                                        dictionary3['row'], batting_against_rows[player_id][team][index]['row'])
                    except KeyError:
                        executor.submit(intermediate, player_id, team, index, dictionary3['temp_player'],
                                        dictionary3['row'], [])
    for player_id, dictionary in data.items():
        for team, dictionary2 in dictionary.items():
            try:
                write_teams_and_stats(player_id, dictionary2, ratios[player_id], team, year)
            except KeyError:
                write_teams_and_stats(player_id, dictionary2, [], team, year)
    logger.log("\t\tTime = " + time_converter(time.time() - bulk_time))
    total_time = time_converter(time.time() - start_time)
    logger.log("Done downloading player images and attributes: time = " + total_time + '\n\n')
    driver_logger.log("\t\tTime = " + total_time)


def extract_player_attributes(player_id, page, reversed_name):
    img_location = str(page.find_all('img')[1]).split('src=')[1].split('/>')[0].split('"')[1]
    if 'gracenote' not in img_location:
        urlretrieve(img_location, os.path.join("..", "..", "interface", "static", "images", "model", "players",
                                               player_id, ".jpg"))
    for ent in page.find_all('div'):
        str_ent = str(ent)
        if 'Throws: </strong>' in str_ent:
            return {'lastName': reversed_name.split(',')[0], 'firstName': reversed_name.split(',')[1],
                    'batsWith': str_ent.split('Bats: </strong>')[1][0], 'primaryPosition': 'N',
                    'throwsWith': str_ent.split('Throws: </strong>')[1][0]}


def intermediate(player_id, team, index, temp_player, row, row2):
    page = load_url(player_id)
    if page is not None:
        if "-0" in temp_player:
            reversed_name = temp_player.split("-0")[0].replace("'", "\'")
        else:
            reversed_name = temp_player.split("0")[0].replace("'", "\'")
        write_to_db(player_id, extract_player_attributes(player_id, page, reversed_name))
    get_stats(player_id, team, index, row, row2)


def get_stats(player_id, team, index, row, row2):
    stats = ["earned_run_average", "W", "L", "G", "GS", "SV", "IP", "H", "R", "ER", "HR", "BB", "IBB", "SO", "HBP",
             "BK", "WP", "whip", "batters_faced"]
    stats2 = ["AB", "2B", "3B", "SB", "CS", "batting_avg_bip", "GIDP", "SH", "SF"]
    stat_dictionary = {}
    for ent in row:
        for stat in stats:
            if '="' + stat + '" >' in ent:
                try:
                    if stat not in ["earned_run_average", "whip", "IP"]:
                        if stat != "batters_faced":
                            stat_dictionary[stat] = int(ent.split('="' + stat + '" >')[1].split('<')[0])
                        else:
                            stat_dictionary['pa'] = int(ent.split('="' + stat + '" >')[1].split('<')[0])
                    else:
                        if stat != "earned_run_average":
                            stat_dictionary[stat] = float(ent.split('="' + stat + '" >')[1].split('<')[0])
                        else:
                            stat_dictionary['era'] = float(ent.split('="' + stat + '" >')[1].split('<')[0])
                except ValueError:
                    pass
                break
    for ent in row2:
        for stat in stats2:
            if '="' + stat + '" >' in ent:
                try:
                    if stat != "batting_avg_bip":
                        stat_dictionary[stat] = int(ent.split('="' + stat + '" >')[1].split('<')[0])
                    else:
                        stat_dictionary[stat] = float(ent.split('="' + stat + '" >')[1].split('<')[0])
                except ValueError:
                    pass
                break
    data[player_id][team][index]['stats'] = stat_dictionary


def load_url(player_id):
    page = None
    db = DatabaseConnection(sandbox_mode)
    if len(db.read('select * from players where playerid = "' + player_id + '";')) == 0:
        page = BeautifulSoup(urlopen("https://www.baseball-reference.com/players/" + player_id[0] + "/" + player_id
                                     + ".shtml"), 'html.parser')
    db.close()
    return page


def write_to_db(player_id, player_attributes):
    fields = ''
    values = ''
    for field, value in player_attributes.items():
        fields += ', ' + field
        values += '", "' + value
    db = DatabaseConnection(sandbox_mode)
    if len(db.read('select * from players where playerid = "' + player_id + '";')) == 0:
        db.write('insert into players (playerid ' + fields + ') values ("' + player_id + values + '");')
    db.close()


def write_teams_and_stats(player_id, stats, ratios, team, year):
    stat_nums = {}
    for index, numbers in stats.items():
        for field, value in numbers['stats'].items():
            if field in stat_nums:
                stat_nums[field] += value
            else:
                stat_nums[field] = value
    db = DatabaseConnection(sandbox_mode)
    if len(db.read('select pt_uniqueidentifier from player_teams where playerid = "' + player_id + '" and teamid = "'
                   + team + '";')) == 0:
        db.write('insert into player_teams (pt_uniqueidentifier, playerid, teamid) values (default, "' + player_id
                 + '", "' + team + '");')
    if len(db.read('select pp_uniqueidentifier from player_pitching where year = ' + str(year) + ' and '
                   'pt_uniqueidentifier = (select pt_uniqueidentifier from player_teams where playerid = "' + player_id
                   + '" and teamid = "' + team + '");')) == 0:
        fields = ''
        values = ''
        for field, value in stat_nums.items():
            fields += ', ' + field
            values += ', ' + str(value)
        for ratio in ratios:
            fields += ', ' + ratio.split(';')[0]
            values += ', ' + ratio.split(';')[1]
        db.write('insert into player_pitching (pp_uniqueidentifier, year, pt_uniqueidentifier, complete_year' + fields
                 + ') values (default, ' + str(year) + ', (select pt_uniqueidentifier from player_teams where playerid'
                 ' = "' + player_id + '" and teamid = "' + team + '"), FALSE' + values + ');')
    db.close()


# for year in range(1996, 2009, 1):
# pitching_constructor(2013)
