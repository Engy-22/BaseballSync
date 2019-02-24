import time
import datetime
from utilities.time_converter import time_converter
from utilities.logger import Logger
from utilities.connections.baseball_data_connection import DatabaseConnection
from urllib.request import urlopen
from bs4 import BeautifulSoup
from utilities.properties import sandbox_mode

logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\all_star_finder.log")


def all_star_finder(year, normal, driver_logger):
    driver_logger.log("\tFinding " + str(year) + " all stars")
    start_time = time.time()
    logger.log("Finding All Stars || Timestamp: " + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
    all_stars = []
    if normal:
        all_star_table = str(BeautifulSoup(urlopen('https://www.baseball-reference.com/allstar/' + str(year)
                                                   + '-allstar-game.shtml'), 'html.parser'))
        nl_table = all_star_table.split('<table>')[2].split('</table>')[0]
        al_table = all_star_table.split('<table>')[1].split('</table>')[0]
        all_stars += get_all_stars(nl_table, '<tr class="">')
        all_stars += get_all_stars(al_table, '<tr class="">')
        write_to_file(year, all_stars)
    else:
        leagues = ['NL', 'AL']
        for league in leagues:
            all_star_table = str(BeautifulSoup(urlopen('https://www.baseball-reference.com/leagues/' + league + '/'
                                                       + str(year) + '-other-leaders.shtml'), 'html.parser'))
            all_star_table1 = all_star_table.split('<h2>League All-Stars</h2>')[1].split('<tbody>')[1].\
                                             split('</tbody')[0]
            all_stars += get_all_stars(all_star_table1, '<tr >')
            if year != 1945:
                all_star_table2 = all_star_table.split('<h2>League All-Stars</h2>')[2].split('<tbody>')[1].\
                                                 split('</tbody')[0]
                all_stars += get_all_stars(all_star_table2, '<tr >')
        write_to_file(year, all_stars)
    total_time = time_converter(time.time() - start_time)
    logger.log("All star finder complete: time = " + total_time)
    driver_logger.log("\t\tTime = " + total_time)


def get_all_stars(table, delimeter):
    rows = table.split(delimeter)
    players = []
    for row in rows:
        try:
            players.append(row.split('<a href="/players/')[1].split('/')[1].split('.shtml')[0])
        except IndexError:
            continue
    return players


def write_to_file(year, all_stars):
    db = DatabaseConnection(sandbox_mode)
    for player in all_stars:
        pt_uids = db.read('select PT_uniqueidentifier from player_teams ' + 'where playerId = "' + player + '";')
        for pt_uid in pt_uids:
            db.write('update player_batting set all_star = TRUE where PT_uniqueidentifier = ' + str(pt_uid[0]) + ' and '
                     'year = ' + str(year) + ';')
            db.write('update player_pitching set all_star = TRUE where PT_uniqueidentifier = ' + str(pt_uid[0]) + ' and'
                     ' year = ' + str(year) + ';')
    db.close()


# all_star_finder(2018, True, Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\"
#                                    "dump.log"))
