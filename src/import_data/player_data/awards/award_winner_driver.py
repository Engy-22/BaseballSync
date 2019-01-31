import time
import datetime
from utilities.time_converter import time_converter
from utilities.logger import Logger
from utilities.baseball_data_connection import DatabaseConnection
from import_data.player_data.awards.mvp_cy_young import mvp_cy_young
from import_data.player_data.awards.roy_gatherer import roy_gatherer
from import_data.player_data.awards.moy_gatherer import moy_gatherer
from import_data.player_data.awards.gold_glove_winners import gold_glove_winners
from import_data.player_data.awards.silver_slugger_winners import silver_slugger_winners
from import_data.player_data.awards.triple_crown_winners import triple_crown_winners
from import_data.player_data.awards.all_star_finder import all_star_finder

logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\"
                "award_winner_driver.log")


def award_winner_driver(year, driver_logger):
    print("gathering award winner data")
    driver_logger.log("\tGathering award winner data")
    start_time = time.time()
    logger.log("Beginning award winner driver || Timestamp: " + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
    t1 = mvp_cy_young(year, logger)
    t2 = roy_gatherer(year, logger)
    t3 = moy_gatherer(year, logger)
    t4, t5 = gold_glove_winners(year, logger)
    t6, t7 = silver_slugger_winners(year, logger)
    t8, t9 = triple_crown_winners(year, logger)
    write_to_file(year, [t1, t2, t3, t4, t5, t6, t7, t8, t9])
    if year >= 1933:
        if year not in [1945, 1959, 1960, 1961, 1962]:
            all_star_finder(year, True, logger)
        else:
            all_star_finder(year, False, logger)
    total_time = time_converter(time.time() - start_time)
    logger.log("Award winner driver complete: time = " + total_time + '\n\n')
    driver_logger.log("\t\tTime = " + total_time)


def write_to_file(year, awards_dict_list):
    if len(awards_dict_list[0]) + len(awards_dict_list[1]) + len(awards_dict_list[2]) + len(awards_dict_list[3]) \
           + len(awards_dict_list[4]) + len(awards_dict_list[5]) > 0:
        db = DatabaseConnection()
        this_string = ""
        for dictionary in awards_dict_list:
            for key, value in dictionary.items():
                this_string += key + ' = "' + value.replace("'", "\'") + '", '
        db.write('update years set ' + this_string[:-2] + ' where year = ' + str(year) + ';')
        db.close()


# dump_logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\dump.log")
# award_winner_driver(2018, dump_logger)
