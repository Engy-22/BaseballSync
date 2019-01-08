import time
from utilities.time_converter import time_converter
from urllib.request import urlopen
from bs4 import BeautifulSoup


def league_pitching_ratios_constructor(year, logger):
    logger.log("\tCalculating pitcher ratios")
    start_time = time.time()
    keys = []
    final_list = {}
    table = str(BeautifulSoup(urlopen("https://www.baseball-reference.com/leagues/MLB/" + str(year) + "-ratio-pitching."
                                      "shtml"), "html.parser")).split('<tbody>')[2].split('</tbody>')[0].split('<tr ')
    for row in table:
        if 'data-append-csv="' in row:
            keys.append(row.split('data-append-csv="')[1].split('"')[0])
            if keys[-1] in keys[:-1]:
                del keys[-1]
                continue
            else:
                final_list[keys[-1]] = []
            gbfb = row.split('data-stat="gb_fb_ratio" >')[1].split('<')[0]
            if len(gbfb) != 0:
                final_list[keys[-1]].append('gb_fb;' + gbfb)
            goao = row.split('data-stat="go_ao_ratio" >')[1].split('<')[0]
            if len(goao) != 0:
                final_list[keys[-1]].append('go_ao;' + goao)
            line_drive_percent = row.split('data-stat="line_drive_perc" >')[1].split('<')[0]
            if '%' in line_drive_percent:
                temp_ldp = line_drive_percent.split('%')[0]
            else:
                temp_ldp = line_drive_percent
            if len(temp_ldp) != 0:
                final_list[keys[-1]].append('ldpercent;' + str(float(temp_ldp)/100))
            gofo_percent = row.split('data-stat="infield_fb_perc" >')[1].split('<')[0]
            if '%' in gofo_percent:
                temp_gofo = gofo_percent.split('%')[0]
            else:
                temp_gofo = gofo_percent
            if len(temp_gofo) != 0:
                final_list[keys[-1]].append('if_fo;' + str(float(temp_gofo)/100))
        else:
            continue
    logger.log("\t\tTime = " + time_converter(time.time() - start_time))
    return final_list


# from utilities.Logger import Logger
# print(league_pitching_ratios_constructor(2018, Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\"
#                                                      "logs\\import_data\\dump.log")))
