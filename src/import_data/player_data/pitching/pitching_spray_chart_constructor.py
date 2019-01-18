import os
import time
import datetime
from urllib.request import urlopen
from bs4 import BeautifulSoup
from utilities.DB_Connect import DB_Connect
from utilities.time_converter import time_converter
from utilities.Logger import Logger
from concurrent.futures import ThreadPoolExecutor

bad_gateway_data = []
logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\"
                "pitching_spray_chart_constructor.log")


def pitcher_spray_chart_constructor(year, driver_logger):
    print("creating pitcher spray charts")
    driver_logger.log("\tCreating pitcher spray charts")
    start_time = time.time()
    logger.log("Downloading " + str(year) + " pitcher spray charts || Timestamp: " + datetime.datetime.today()\
               .strftime('%Y-%m-%d %H:%M:%S'))
    if year >= 1988:
        db, cursor = DB_Connect.grab("baseballData")
        pt_uid_players = set(DB_Connect.read(cursor, 'select PT_uniqueidentifier from player_pitching where year = '
                                                     + str(year) + ';'))
        DB_Connect.close(db)
        with ThreadPoolExecutor(os.cpu_count()) as executor:
            for ent in pt_uid_players:
                executor.submit(reduce_functionality, year, ent)
    else:
        logger.log("\tNo spray chart data before 1988")
    if len(bad_gateway_data) > 0:
        revisit_bad_gateways(year, bad_gateway_data)
    total_time = time_converter(time.time() - start_time)
    logger.log("Done downloading pitcher spray charts: time = " + total_time + '\n\n')
    driver_logger.log("\t\tTime = " + total_time)


def reduce_functionality(year, ent):
    stats1 = {"To Infield_PA": ["pa_infield", ""],
              "To Outfield_PA": ["pa_outfield", ""],
              "To Infield_H": ["infield_hits", ""],
              "Ball In Play_PA": ["pa_balls_in_play", ""],
              "Pulled_PA_R": ["r_pa_pulled", ""],
              "Up Mdle_PA_R": ["r_pa_middle", ""],
              "Opp Fld_PA_R": ["r_pa_oppo", ""],
              "To Infield_AB": ["ab_infield", ""],
              "To Outfield_AB": ["ab_outfield", ""],
              "Ball In Play_AB": ["ab_balls_in_play", ""],
              "Pulled_AB_R": ["r_ab_pulled", ""],
              "Up Mdle_AB_R": ["r_ab_middle", ""],
              "Opp Fld_AB_R": ["r_ab_oppo", ""],
              "Pulled_H_R": ["r_h_pull", ""],
              "Up Mdle_H_R": ["r_h_middle", ""],
              "Opp Fld_H_R": ["r_h_oppo", ""],
              "Pulled_2B_R": ["r_double_pull", ""],
              "Up Mdle_2B_R": ["r_double_middle", ""],
              "Opp Fld_2B_R": ["r_double_oppo", ""],
              "Pulled_3B_R": ["r_triple_pull", ""],
              "Up Mdle_3B_R": ["r_triple_middle", ""],
              "Opp Fld_3B_R": ["r_triple_oppo", ""],
              "Pulled_HR_R": ["r_hr_pull", ""],
              "Up Mdle_HR_R": ["r_hr_middle", ""],
              "Opp Fld_HR_R": ["r_hr_oppo", ""],
              "Pulled_PA_L":["l_pa_pulled", ""],
              "Up Mdle_PA_L": ["l_pa_middle", ""],
              "Opp Fld_PA_L": ["l_pa_oppo", ""],
              "Pulled_AB_L": ["l_ab_pulled", ""],
              "Up Mdle_AB_L": ["l_ab_middle", ""],
              "Opp Fld_AB_L": ["l_ab_oppo", ""],
              "Pulled_H_L": ["l_h_pull", ""],
              "Up Mdle_H_L": ["l_h_middle", ""],
              "Opp Fld_H_L": ["l_h_oppo", ""],
              "Pulled_2B_L": ["l_double_pull", ""],
              "Up Mdle_2B_L": ["l_double_middle", ""],
              "Opp Fld_2B_L": ["l_double_oppo", ""],
              "Pulled_3B_L": ["l_triple_pull", ""],
              "Up Mdle_3B_L": ["l_triple_middle", ""],
              "Opp Fld_3B_L": ["l_triple_oppo", ""],
              "Pulled_HR_L": ["l_hr_pull", ""],
              "Up Mdle_HR_L": ["l_hr_middle", ""],
              "Opp Fld_HR_L": ["l_hr_oppo", ""]}
    stats2 = {"Ground_Balls_PA": "",
              "Ground_Balls_AB": "",
              "Ground_Balls_H": "",
              "Ground_Balls_2B": "",
              "Ground_Balls_3B": "",
              "Ground_Balls_HR": "",
              "Fly_Balls_PA": "",
              "Fly_Balls_AB": "",
              "Fly_Balls_H": "",
              "Fly_Balls_2B": "",
              "Fly_Balls_3B": "",
              "Fly_Balls_HR": "",
              "Line_Drives_PA": "",
              "Line_Drives_AB": "",
              "Line_Drives_H": "",
              "Line_Drives_2B": "",
              "Line_Drives_3B": "",
              "Line_Drives_HR": ""}
    db, cursor = DB_Connect.grab("baseballData")
    player_id = DB_Connect.read(cursor, "select playerId from player_teams where PT_uniqueidentifier = " + str(ent[0])
                                + ";")[0][0]
    if DB_Connect.read(cursor, 'select pa_infield from player_pitching where PT_uniqueidentifier = ' + str(ent[0])
                               + ' and year = ' + str(year) + ';')[0][0] is None:
        try:
            page = str(BeautifulSoup(urlopen('https://www.baseball-reference.com/players/split.fcgi?id=' + player_id
                                             + '&year=' + str(year) + '&t=p'), 'html.parser'))
        except Exception:
            global bad_gateway_data
            bad_gateway_data.append([ent, stats1, stats2])
            return
        try:
            table_rows1 = page.split('<h2>Hit Location</h2>')[1].split('<tbody>')[1].split('</tbody>')[0].split('<tr')
            for row in table_rows1:
                for key, value in stats1.items():
                    if key.split('_')[0] in row:
                        if key not in ["To Infield_PA", "To Outfield_PA", "To Infield_H", "Ball In Play_PA"]:
                            if key[-1] != row.split('HB</th><td')[0][-1]:
                                continue
                        try:
                            stats1[key][1] = row.split('data-stat="' + key.split('_')[1] + '" >')[1].split('<')[0]
                        except IndexError:
                            continue
            table_rows2 = page.split('<h2>Hit Trajectory</h2>')[1].split('<tbody>')[1].split('</tbody>')[0].split('<tr')
            for row in table_rows2:
                for key, value in stats2.items():
                    this_key = key.split('_')
                    if this_key[0] + ' ' + this_key[1] in row:
                        try:
                            stats2[key] = row.split('data-stat="' + key.split('_')[2] + '" >')[1].split('<')[0]
                        except IndexError:
                            continue
        except IndexError:
            for key, value in stats1.items():
                stats1[key][1] = '0'
            for key, value in stats2.items():
                stats2[key] = '0'
        finally:
            write_to_file(year, ent[0], stats1, stats2)


def write_to_file(year, pt_uid, stat_list1, stat_list2):
    db, cursor = DB_Connect.grab("baseballData")
    query_string = ""
    for key, value in stat_list1.items():
        if value[1] == '':
            value[1] = '0'
        query_string += value[0] + ' = ' + value[1] + ', '
    for key, value in stat_list2.items():
        if value == '':
            value = '0'
        query_string += key + ' = ' + value + ', '
    DB_Connect.write(db, cursor, 'update player_pitching set ' + query_string[:-2] + ' where PT_uniqueidentifier = '
                                 + str(pt_uid) + ' and year = ' + str(year) + ';')
    DB_Connect.close(db)


def revisit_bad_gateways(year, data):
    for datum in data:
        reduce_functionality(year, datum[0])


# dump_logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\dump.log")
# pitcher_spray_chart_constructor(2018, dump_logger)
