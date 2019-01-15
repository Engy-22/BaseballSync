import time
import datetime
from utilities.get_oldest_year import get_oldest_year
from import_data.team_data.team_ranker_year import team_ranker_year
from import_data.team_data.team_ranker_ovr import team_ranker_ovr
from statistics import stdev, mean
from utilities.time_converter import time_converter
from utilities.Logger import Logger

logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\rank_driver.log")


def rank_driver(year, driver_logger):
    print("calculating team ranks (year)")
    driver_logger.log("\tBeginning rank driver")
    start_time = time.time()
    logger.log("Beginning rank driver || Timestamp: " + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
    logger.log("\tCalculating team ranks (year)")
    runs = {}
    allowed = {}
    difference = {}
    standard_deviation = {}
    driver_logger.log("\t\tCalculating team ranks (year)")
    for data_year in range(year, get_oldest_year(driver_logger)-1, -1):
        runs[data_year], allowed[data_year], difference[data_year] = team_ranker_year(data_year)
        standard_deviation[str(data_year)] = stdev([team_runs[1] for team_runs in runs[data_year]])
    total_time = time_converter(time.time() - start_time)
    logger.log("\t\tTime = " + total_time)
    driver_logger.log("\t\t\tTime = " + total_time)
    second_time = time.time()
    driver_logger.log("\t\tCalculating team ranks (overall)")
    logger.log("\tCalculating team ranks (overall)")
    print("calculating team ranks (overall)")
    total_list = []
    years = [value for key, value in runs.items()]
    for ent in years:
        for team_total in ent:
            total_list.append(team_total[1])
    average_deviation = mean([value for key, value in standard_deviation.items()])
    team_ranker_ovr(runs, True, "offRank_ovr", standard_deviation, average_deviation)
    team_ranker_ovr(allowed, False, "defRank_ovr", standard_deviation, average_deviation)
    team_ranker_ovr(difference, True, "ovrRank_ovr", standard_deviation, average_deviation)
    second = time_converter(time.time() - second_time)
    logger.log("\t\tTime = " + second)
    driver_logger.log("\t\t\tTime = " + second)
    total_time = time_converter(time.time() - start_time)
    logger.log("Rank driver complete: time = " + total_time + '\n\n')
    driver_logger.log("\t\tRank driver time = " + total_time)


# rank_driver(2018, Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\dump.log"))
