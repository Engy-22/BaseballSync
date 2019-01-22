import os
from utilities.logger import Logger
import time
import datetime
from utilities.dbconnect import DatabaseConnection
from utilities.time_converter import time_converter
from concurrent.futures import ThreadPoolExecutor
from urllib.request import urlopen, urlretrieve
from bs4 import BeautifulSoup


pages = {}
logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\"
                "ballpark_and_manager_data.log")


def ballpark_and_manager_data(year, driver_logger):
    driver_logger.log('\tGathering ballpark and manager data')
    print("Gathering ballpark and manager data")
    start_time = time.time()
    global pages
    pages = {}
    logger.log('Beginning ballpark and manager data download for ' + str(year) + ' || Timestamp: '
               + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
    teams = {}
    with open('C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\background\\yearTeams.txt', 'rt') as file:
        for line in file:
            if str(year) in line:
                temp_line = line.split(',')[1:-1]
                for team in temp_line:
                    temp_team = team.split(';')
                    if 'TOT' not in temp_team:
                        teams[temp_team[1]] = temp_team[0]
                break
    logger.log('Begin downloading team pages')
    download_time = time.time()
    with ThreadPoolExecutor(os.cpu_count()) as executor1:
        for team_key, team_id in teams.items():
            executor1.submit(load_url, year, team_key)
    logger.log('\tDone downloading team pages: time = ' + time_converter(time.time() - download_time))
    logger.log("Calculating and writing ballpark numbers and downloading images")
    calc_and_download_time = time.time()
    team_count = len(teams)
    with ThreadPoolExecutor(os.cpu_count()) as executor2:
        for team_key, team_id in teams.items():
            executor2.submit(gather_team_home_numbers, team_id, team_key, year, team_count)
    logger.log("\tDone calculating and writing ballpark numbers and downloading manager data: time = "
               + time_converter(time.time() - calc_and_download_time))
    total_time = time_converter(time.time() - start_time)
    logger.log('Ballpark and manager data download completed: time = ' + total_time + '\n\n')
    driver_logger.log('\t\tTime = ' + total_time)


def gather_team_home_numbers(team_id, team_key, year, team_count):
    logger.log("\tCalculating home numbers and downloading manager data: " + team_id)
    db = DatabaseConnection()
    if len(db.read('select BY_uniqueidentifier from ballpark_years where teamId = "' + team_id + '" and year = '
                   + str(year) + ';')) == 0:
        global pages
        table = str(pages[team_key])
        manager_ids = {}
        try:
            manager_data = table.split('<strong>Manager')[1].split('<p>')[0].split('<a href="/managers/')[1:]
            home_row = table.split('<h2>Home or Away</h2>')[1].split('<tbody>')[1].split('</tbody>')[0]\
                .split('<tr >')[1]
            ballpark_platoon_splits = table.split('<h2>Hit Location</h2>')[1].split('<tbody>')[1].split('</tbody>')[0]\
                .split('<tr >')
            hit_trajectory_table = table.split('<h2>Hit Trajectory</h2>')[1].split('<tbody>')[1].split('</tbody>')[0]\
                .split('<tr >')
            stats = ['AB', 'H', '2B', '3B', 'HR']
            overall_stats = {}
            home_percent = {}
            season_total_row = table.split('<h2>Season Totals</h2>')[1].split('<tbody>')[1].split('</tbody>')[0].\
                split('<tr>')[1]
            for stat in stats:
                league_total = int(db.read("select " + stat + " from years where year = " + str(year) + ";")[0][0])
                team_total = int(season_total_row.split('data-stat="' + stat + '">')[1].split('<')[0])
                overall_stats[stat] = team_total / ((league_total - team_total) / (team_count - 2))
                home = int(home_row.split('data-stat="' + stat + '" >')[1].split('<')[0])
                home_percent[stat] = home / team_total
            db.close()
            stat_translate = {'AB': 'AB', 'H': 'H', '2B': 'double', '3B': 'triple', 'HR': 'homerun'}
            r_location = {'Up Mdle': 'centerfield', 'Opp Fld': 'rightfield', 'Pulled': 'leftfield'}
            l_location = {'Up Mdle': 'centerfield', 'Opp Fld': 'leftfield', 'Pulled': 'rightfield'}
            location = {'AB_centerfield': 0, 'AB_leftfield': 0, 'AB_rightfield': 0,
                        'H_centerfield': 0, 'H_leftfield': 0, 'H_rightfield': 0,
                        'double_centerfield': 0, 'double_leftfield': 0, 'double_rightfield': 0,
                        'triple_centerfield': 0, 'triple_leftfield': 0, 'triple_rightfield': 0,
                        'homerun_centerfield': 0, 'homerun_leftfield': 0, 'homerun_rightfield': 0}
            trajectory = {"Ground_Balls_PA": 0, "Ground_Balls_AB": 0, "Ground_Balls_H": 0, "Ground_Balls_2B": 0,
                          "Ground_Balls_3B": 0, "Ground_Balls_HR": 0, "Fly_Balls_PA": 0, "Fly_Balls_AB": 0,
                          "Fly_Balls_H": 0, "Fly_Balls_2B": 0, "Fly_Balls_3B": 0, "Fly_Balls_HR": 0,
                          "Line_Drives_PA": 0, "Line_Drives_AB": 0, "Line_Drives_H": 0, "Line_Drives_2B": 0,
                          "Line_Drives_3B": 0, "Line_Drives_HR": 0}
            for row in ballpark_platoon_splits:
                if '-RHB' in row:
                    handedness = "R"
                elif '-LHB' in row:
                    handedness = "L"
                else:
                    continue
                for stat in stats:
                    if handedness == "R":
                        location[stat_translate[stat] + '_' + r_location[row.split('data-stat="split_name" >')[1]
                            .split('-')[0]]] += (int(row.split('data-stat="' + stat + '" >')[1].split('<')[0])
                                                 * home_percent[stat]) / overall_stats[stat]
                    else:
                        location[stat_translate[stat] + '_' + l_location[row.split('data-stat="split_name" >')[1]
                            .split('-')[0]]] += (int(row.split('data-stat="' + stat + '" >')[1].split('<')[0])
                                                 * home_percent[stat]) / overall_stats[stat]
            for row in hit_trajectory_table:
                for key, value in trajectory.items():
                    if key.split('_')[0] + ' ' + key.split('_')[1] in row:
                        stat = key.split('_')[-1]
                        if stat != 'PA':
                            trajectory[key] = int(row.split('data-stat="' + key.split('_')[2] + '" >')[1].
                                                  split('<')[0]) * home_percent[stat]
        except IndexError:
            table = str(BeautifulSoup(urlopen('https://www.baseball-reference.com/teams/' + team_key + '/' + str(year)
                                              + '.shtml'), 'html.parser'))
            manager_data = table.split('<strong>Manager')[1].split('<p>')[0].split('<a href="/managers/')[1:]
            location = {'AB_centerfield': -1, 'AB_leftfield': -1, 'AB_rightfield': -1,
                        'H_centerfield': -1, 'H_leftfield': -1, 'H_rightfield': -1,
                        'double_centerfield': -1, 'double_leftfield': -1, 'double_rightfield': -1,
                        'triple_centerfield': -1, 'triple_leftfield': -1, 'triple_rightfield': -1,
                        'homerun_centerfield': -1, 'homerun_leftfield': -1, 'homerun_rightfield': -1}
            trajectory = {"Ground_Balls_PA": -1, "Ground_Balls_AB": -1, "Ground_Balls_H": -1, "Ground_Balls_2B": -1,
                          "Ground_Balls_3B": -1, "Ground_Balls_HR": -1, "Fly_Balls_PA": -1, "Fly_Balls_AB": -1,
                          "Fly_Balls_H": -1, "Fly_Balls_2B": -1, "Fly_Balls_3B": -1, "Fly_Balls_HR": -1,
                          "Line_Drives_PA": -1, "Line_Drives_AB": -1, "Line_Drives_H": -1, "Line_Drives_2B": -1,
                          "Line_Drives_3B": -1, "Line_Drives_HR": -1}
        manager_page = str(BeautifulSoup(urlopen('https://www.baseball-reference.com/managers/' + manager_data[0]
                                                 .split('.shtml')[0] + '.shtml'), 'html.parser'))
        try:
            manager_pic_url = manager_page.split('<img class="" src="')[1].split('"')[0]
            urlretrieve(manager_pic_url, "C:\\Users\\Anthony Raimondo\\images\\managers\\" + manager_data[0].
                        split('.shtml')[0] + ".jpg")
        except IndexError as e:
            logger.log('\t\t' + str(e))
        team_pic_url = table.split('<div class="media-item logo loader">')[1].split('<')[1].split('src="')[1].split('"')[0]
        try:
            urlretrieve(team_pic_url, "C:\\Users\\Anthony Raimondo\\images\\teams\\" + team_id + str(year) + ".jpg")
        except Exception as e:
            logger.log('\t\t' + str(e))
        for i in manager_data:
            manager_ids[i.split('.shtml')[0]] = i.split('(')[1].split(')')[0]
        try:
            park_name = table.split('<strong>Ballpark')[1].split('</strong> ')[1].split('</p>')[0][:-1]
        except IndexError as e:
            logger.log('\t\t' + str(e))
            park_name = "No Home Field"
        write_to_db(team_id, location, trajectory, manager_ids, year, park_name)


def load_url(year, team_key):
    logger.log("\tDownloading " + team_key + " data")
    global pages
    pages[team_key] = BeautifulSoup(urlopen('https://www.baseball-reference.com/teams/split.cgi?t=p&team=' + team_key
                                            + '&year=' + str(year)), 'html.parser')


def write_to_db(team_id, stats, trajectory, manager_ids, year, park_name):
    db = DatabaseConnection()
    if len(db.read('select parkId from ballparks where parkName = "' + park_name + '";')) == 0:
        db.write('insert into ballparks (parkId, parkName) values (default, "' + park_name + '");')
    if len(db.read('select BY_uniqueidentifier from ballpark_years where teamId = "' + team_id + '" and year = '
                   + str(year) + ';')) == 0:
        field_list = ""
        value_list = ""
        for key, value in stats.items():
            field_list += key + ', '
            value_list += str(value) + ', '
        for key, value in trajectory.items():
            field_list += key + ', '
            value_list += str(value) + ', '
        db.write('insert into ballpark_years (BY_uniqueidentifier, teamId, year, ' + field_list + 'parkId) values '
                 '(default, "' + team_id + '", ' + str(year) + ', ' + value_list + '(select parkId from ballparks '
                 'where parkName = "' + park_name + '"));')
    for manager, record in manager_ids.items():
        if len(db.read('select MT_uniqueidentifier from manager_teams where managerId = "' + manager + '" and teamId '
                       '= "' + team_id + '";')) == 0:
            db.write('insert into manager_teams (MT_uniqueidentifier, managerId, teamId) values (default, "' + manager
                     + '", "' + team_id + '");')
        if len(db.read('select MY_uniqueidentifier from manager_year where year = ' + str(year) + ' and '
                       'MT_uniqueidentifier = (select MT_uniqueidentifier from manager_teams where managerId = "'
                       + manager + '" and teamId = "' + team_id + '")' + ';')) == 0:
            db.write('insert into manager_year (MY_uniqueidentifier, year, MT_uniqueidentifier, wins, loses) values '
                     '(default, ' + str(year) + ', (select MT_uniqueidentifier from manager_teams where managerId = "'
                     + manager + '" and teamId = "' + team_id + '")' + ', ' + record.split('-')[0] + ', '
                     + record.split('-')[1] + ');')
    db.close()


# dump_logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\dump.log")
# for year in range(1996, 1997, 1):
#     ballpark_and_manager_data(year, dump_logger)
