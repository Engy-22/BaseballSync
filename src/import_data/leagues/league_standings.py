import os
import time
import datetime
from urllib.request import urlopen
from bs4 import BeautifulSoup
from utilities.database.wrappers.baseball_data_connection import DatabaseConnection
from utilities.translate_team_id import translate_team_id
from utilities.logger import Logger
from utilities.time_converter import time_converter
from utilities.properties import sandbox_mode, import_driver_logger as driver_logger

logger = Logger(os.path.join("..", "..", "baseball-sync", "logs", "import_data", "league_standings.log"))


def league_standings(year):
    driver_logger.log("\tAdding to team_years (standings)")
    print("Adding to team_years (standings)")
    start_time = time.time()
    logger.log('Begin organizing league standings for ' + str(year) + ' || Timestamp: ' + datetime.datetime.today().
               strftime('%Y-%m-%d %H:%M:%S'))
    page = str(BeautifulSoup(urlopen("https://www.baseball-reference.com/leagues/MLB/" + str(year)
                                     + "-standings.shtml"), "html.parser"))
    try:
        playoffs = page.split('<h2>Postseason</h2>')[1].split('</tbody></table>')[0]
    except IndexError:
        logger.log("\tNo playoffs in " + str(year))
        playoffs = ""
    divisions = {}
    if year != 1981:
        try:
            divisions['al_east'] = page.split('<h2>East Division</h2>')[1].split('<tbody>')[1].\
                split('</tbody></table>')[0]
        except IndexError:
            pass
        try:
            divisions['nl_east'] = page.split('<h2>East Division</h2>')[2].split('<tbody>')[1].\
                split('</tbody></table>')[0]
        except IndexError:
            pass
        try:
            divisions['al_central'] = page.split('<h2>Central Division</h2>')[1].split('<tbody>')[1].\
                split('</tbody></table>')[0]
        except IndexError:
            pass
        try:
            divisions['nl_central'] = page.split('<h2>Central Division</h2>')[2].split('<tbody>')[1].\
                split('</tbody></table>')[0]
        except IndexError:
            pass
        try:
            divisions['al_west'] = page.split('<h2>West Division</h2>')[1].split('<tbody>')[1].\
                split('</tbody></table>')[0]
        except IndexError:
            pass
        try:
            divisions['nl_west'] = page.split('<h2>West Division</h2>')[2].split('<tbody>')[1].\
                split('</tbody></table>')[0]
        except IndexError:
            pass
    else:
        try:
            divisions['al_east'] = page.split('<h2>East Division -- Overall</h2>')[1].split('<tbody>')[1]\
                .split('</tbody>' + '</table>')[0]
        except IndexError:
            pass
        try:
            divisions['al_west'] = page.split('<h2>West Division -- Overall</h2>')[1].split('<tbody>')[1].\
                split('</tbody>' + '</table>')[0]
        except IndexError:
            pass
        try:
            divisions['nl_east'] = page.split('<h2>East Division -- Overall</h2>')[2].split('<tbody>')[1]\
                .split('</tbody>' + '</table>')[0]
        except IndexError:
            pass
        try:
            divisions['nl_west'] = page.split('<h2>West Division -- Overall</h2>')[2].split('<tbody>')[1].\
                split('</tbody>' + '</table>')[0]
        except IndexError:
            pass
    main_table = page.split('<div class="overthrow table_container" id="div_expanded_standings_overall">')[1].\
        split('<tbody>')[1].split('<tr class="league_average_table')[0].split('<tr')
    champs = {}
    for row in main_table:
        if year == 1904 or year < 1903:
            if 'data-stat="lg_ID" ><strong>' in row:
                champs[row.split('data-stat="lg_ID" ><strong>')[1].split('<')[0]] = \
                    translate_team_id(row.split('href="/teams/')[1].split('/')[0], year)
        try:
            team_key = row.split('/teams/')[1].split('/')[0]
            team_id = translate_team_id(team_key, year)
            if year > 1968:
                this_string = "'" + team_id + "'," + str(year) + "," + get_league_division(divisions, team_key, year)
            else:
                this_string = "'" + team_id + "'," + str(year) + "," + get_league_only(row)
            this_string += ',' + wins_loses(row)
            this_string += ',' + is_in_playoffs(playoffs, team_key, year)
        except IndexError:
            continue
        write_to_db(this_string, team_id, year)
    if year == 1903 or year > 1904:  # the first world series (1903); didn't play a WS in 1904
        series = {'World Series': 1, 'ALCS': 1, 'NLCS': 1, 'AL Division Series': 2, 'NL Division Series': 2}
        abbreviation = {'World Series': 'ws', 'ALCS': 'alcs', 'NLCS': 'nlcs', 'AL Division Series': 'alds',
                        'NL Division Series': 'alds'}
        playoff_picture = {}
        for matchup, times in series.items():
            for instance in range(times):
                try:
                    playoff_picture[abbreviation[matchup]+'_champ' + str(instance+1)] = translate_team_id(playoffs.
                        split('>' + matchup + '<')[1]. split('a href="/teams/')[1].split('/')[0], year)
                except IndexError:
                    playoff_picture[abbreviation[matchup]+'_champ' + str(instance+1)] = None
                try:
                    playoff_picture[abbreviation[matchup]+'_runnerup' + str(instance+1)] = translate_team_id(playoffs.
                        split('>' + matchup + '<')[1].split('a href="/teams/')[2].split('/')[0], year)
                except IndexError:
                    playoff_picture[abbreviation[matchup]+'_runnerup' + str(instance+1)] = None
        write_playoff_data(year, playoff_picture)
    else:
        write_league_champs_non_ws(champs, year)
    total_time = time_converter(time.time() - start_time)
    logger.log('Done organizing league standings for ' + str(year) + ': time = ' + total_time + '\n\n')
    driver_logger.log("\t\tTime = " + total_time)


def write_league_champs_non_ws(champs, year):
    db = DatabaseConnection(sandbox_mode)
    sets = ''
    team_count = 0
    for league, team in champs.items():
        if league in ['AL', 'NL']:
            sets += league + 'cs_champ = "' + team + '", '
        else:
            sets += league + '_champ = "' + team + '", '
        team_count += 1
    if len(sets) > 0:
        sets = 'update years set ' + sets[:-2] + ' where year = ' + str(year) + ';'
    db.write(sets)
    db.close()


def write_playoff_data(year, playoff_data):
    db = DatabaseConnection(sandbox_mode)
    sets = ''
    for accomplishment, team_id in playoff_data.items():
        if team_id is not None:
            if 'lds' not in accomplishment:
                sets += accomplishment[:-1] + ' = "' + team_id + '", '
            else:
                sets += accomplishment + ' = "' + team_id + '", '
    if len(sets) > 0:
        db.write('update years set ' + sets[:-2] + ' where year = ' + str(year) + ';')
    db.close()


def get_league_division(divisions, team_key, year):
    divs = ['al_east', 'al_central', 'al_west', 'nl_east', 'nl_central', 'nl_west']
    for div in divs:
        try:
            if team_key in divisions[div]:
                if year > 1968:
                    return "'" + div[:2] + "','" + div[3] + "'"
                else:
                    return "'" + div[:2] + "',N"
        except KeyError:
            continue


def get_league_only(row):
    try:
        return "'" + row.split('data-stat="lg_ID" ><strong>')[1].split('</strong>')[0] + "','N'"
    except IndexError:
        return "'" + row.split('data-stat="lg_ID" >')[1].split('</td><td')[0] + "','N'"


def wins_loses(row):
    try:
        temp_string = row.split('data-stat="W" ><strong>')[1].split('<')[0] + ','
    except IndexError:
        return row.split('data-stat="W" >')[1].split('<')[0] + ',' + row.split('data-stat="L" >')[1].split('<')[0]
    try:
        temp_string += row.split('data-stat="L" ><strong>')[1].split('<')[0]
    except IndexError:
        temp_string += row.split('data-stat="L" >')[1].split('<')[0]
    return temp_string


def is_in_playoffs(playoffs, team_key, year):
    is_in = "FALSE"
    if 1904 != year >= 1903:  # the first world series (1903); didn't play a WS in 1904
        series = playoffs.split('<tr>')
        for matchup in series:
            try:
                if matchup.split('/teams/')[1].split('/')[0] == team_key:
                    return "TRUE"
                if matchup.split('/teams/')[2].split('/')[0] == team_key:
                    return "TRUE"
            except IndexError:
                continue
    return is_in


def write_to_db(this_string, team_id, year):
    logger.log("\tWriting " + team_id + " to team_years")
    db = DatabaseConnection(sandbox_mode)
    if len(db.read('select TY_uniqueidentifier from team_years where teamId = "' + team_id + '" and year = '
                   + str(year) + ';')) == 0:
        db.write('Insert into team_years (TY_uniqueidentifier, teamId, year, league, division, wins, loses, playoffs, '
                 'BY_uniqueidentifier) values (default,' + this_string + ', (select BY_uniqueidentifier from '
                 'ballpark_years where teamId = "' + team_id + '" and year = ' + str(year) + '));')
    db.close()


# league_standings(2018)
