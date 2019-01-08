import os
import time
import datetime
from urllib.request import urlopen, urlretrieve
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from utilities.DB_Connect import DB_Connect
from import_data.player_data.pitching.league_pitching_ratios_constructor import league_pitching_ratios_constructor
from utilities.translate_team_id import translate_team_id
from utilities.time_converter import time_converter
from utilities.Logger import Logger

data = {}

logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\pitchers.log")


def pitching_constructor(year, driver_logger):
    print('Downloading pitcher images and attributes')
    driver_logger.log("\tDownloading pitcher images and attributes")
    start_time = time.time()
    logger.log("Downloading pitcher " + str(year) + " data || Timestamp: " + datetime.datetime.today()\
               .strftime('%Y-%m-%d %H:%M:%S'))
    logger.log("\tAssembling list of players")
    table = str(BeautifulSoup(urlopen("https://www.baseball-reference.com/leagues/MLB/" + str(year)
                                      + "-standard-pitching.shtml"), 'html.parser')).\
        split('<table class="sortable stats_table" id')[1].split('<tbody>')[1].split('</tbody>')[0].split('<tr')
    count = 0
    for row in table:
        if 'data-stat="player" csk="' in row and 'data-append-csv="' in row:
            data[str(count)] = {'player_id': row.split('data-append-csv="')[1].split('"')[0].replace("'", "\'"),
                                'row': row.split('data-stat'),
                                'temp_player': row.split('data-stat="player" csk="')[1].split('" >')[0]}
            count += 1
    table2 = str(BeautifulSoup(urlopen("https://www.baseball-reference.com/leagues/MLB/" + str(year) + "-batting-"
                                       "pitching.shtml"), 'html.parser')).split('<tr class="full_table non_qual" >'
                                                                                '<th scope="row"')
    batting_against_rows = {}
    for row in table2:
        if 'data-stat="player" csk="' in row:
            this_id = row.split('data-append-csv="')[1].split('"')[0]
            if this_id not in batting_against_rows:
                batting_against_rows[this_id] = row.split('<td')
    logger.log("\t\tDone assembling list of players")
    bulk_time = time.time()
    ratios = league_pitching_ratios_constructor(year, logger)
    logger.log("\tParsing player pages, downloading images, and extracting player attributes")
    with ThreadPoolExecutor(os.cpu_count()) as executor:
        for id_num, dictionary in data.items():
            player_id = dictionary['player_id']
            try:
                executor.submit(intermediate, id_num, year, player_id, dictionary['temp_player'], dictionary['row'],
                                batting_against_rows[player_id])
            except KeyError:
                executor.submit(intermediate, id_num, year, player_id, dictionary['temp_player'], dictionary['row'], [])
    for id_num, dictionary in data.items():
        player_id = dictionary['player_id']
        try:
            write_teams_and_stats(player_id, dictionary['stats'], ratios[player_id], dictionary['team'], year)
        except KeyError:
            write_teams_and_stats(player_id, dictionary['stats'], [], dictionary['team'], year)
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


def get_team_and_stats(id_num, row, row2, year):
    stats = ["W", "L", "G", "GS", "SV", "IP", "H", "R", "ER", "HR", "BB", "IBB", "SO", "HBP", "BK", "WP", "whip",
             "team_ID"]
    stats2 = ["PA", "AB", "2B", "3B", "SB", "CS", "batting_avg_bip", "GIDP", "SH", "SF"]
    stat_dictionary = {}
    team = "TOT"
    for ent in row:
        for stat in stats:
            if '="' + stat + '" >' in ent:
                try:
                    if stat not in ["pitching_avg", "onbase_perc", "slugging_perc", "onbase_plus_slugging", "team_ID"]:
                        stat_dictionary[stat] = int(ent.split('="' + stat + '" >')[1].split('<')[0])
                    elif stat == "team_ID":
                        try:
                            team = translate_team_id(ent.split('a href="/teams/')[1].split('/')[0], year)
                        except IndexError:
                            pass
                    else:
                        stat_dictionary[stat] = float(ent.split('="' + stat + '" >')[1].split('<')[0])
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


def write_teams_and_stats(player_id, stats, ratios, team, year):
    db, cursor = DB_Connect.grab("baseballData")
    if len(DB_Connect.read(cursor, 'select pt_uniqueidentifier from player_teams where playerid = "' + player_id
                                   + '" and teamid = "' + team + '";')) == 0:
        DB_Connect.write(db, cursor, 'insert into player_teams (pt_uniqueidentifier, playerid, teamid) values (default,'
                                     ' "' + player_id + '", "' + team + '");')
    if len(DB_Connect.read(cursor, 'select pp_uniqueidentifier from player_pitching where year = ' + str(year)
                                   + ' and pt_uniqueidentifier = (select pt_uniqueidentifier from player_teams where '
                                   'playerid = "' + player_id + '" and teamid = "' + team + '");')) == 0:
        fields = ''
        values = ''
        for field, value in stats.items():
            fields += ', ' + field
            values += ', ' + str(value)
        for ratio in ratios:
            fields += ', ' + ratio.split(';')[0]
            values += ', ' + ratio.split(';')[1]
        DB_Connect.write(db, cursor, 'insert into player_pitching (pp_uniqueidentifier, year, pt_uniqueidentifier, '
                                     'complete_year' + fields + ') values (default, ' + str(year) + ', (select '
                                     'pt_uniqueidentifier from player_teams where playerid = "' + player_id + '" and '
                                     'teamid = "' + team + '"), FALSE' + values + ');')
    DB_Connect.close(db)


def intermediate(id_num, year, player_id, temp_player, row, row2):
    page = load_url(player_id)
    if page is not None:
        if "-0" in temp_player:
            reversed_name = temp_player.split("-0")[0].replace("'", "\'")
        else:
            reversed_name = temp_player.split("0")[0].replace("'", "\'")
        write_to_db(player_id, extract_player_attributes(player_id, page, reversed_name))
    get_team_and_stats(id_num, row, row2, year)


# pitching_constructor(2018, Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\"
#                                   "dump.log"))
