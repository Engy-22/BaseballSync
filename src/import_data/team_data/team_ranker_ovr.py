import time
from utilities.dbconnect import DatabaseConnection
from utilities.translate_team_id import translate_team_id
from statistics import mean
from utilities.time_converter import time_converter
from utilities.Logger import Logger


def team_ranker_ovr(data, greater_than, field, standard_deviation, average_deviation):
    logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\"
                    "team_ranker_ovr.log")
    logger.log("Calculating overall team ranks: " + field)
    start_time = time.time()
    final_data = {}
    if field != "ovrRank_ovr":
        for year, value in data.items():
            year_average = mean([ent[1] for ent in value])
            final_data[year] = []
            for ent in value:
                # if field == "offRank_ovr":
                #     final_data[key].append([ent[0], (ent[1] * average_deviation) /
                #                            (year_average * standard_deviation[str(key)])])
                # else:
                #     final_data[key].append([ent[0], (ent[1] * standard_deviation[str(key)]) /
                #                            (year_average * average_deviation)])
                if field == "offRank_ovr":
                    final_data[year].append([ent[0], (ent[1] / year_average) - (average_deviation
                                                                                - standard_deviation[str(year)])])
                else:
                    final_data[year].append([ent[0], (ent[1] / year_average) + (average_deviation
                                                                                - standard_deviation[str(year)])])
        # write_to_file(final_data, greater_than, field)
    else:
        for year, value in data.items():
            # year_average = mean([ent[1] for ent in value])
            final_data[year] = []
            for ent in value:
                for team_value in data[year]:
                    if team_value[0] == ent[0]:
                        # final_data[key].append([ent[0], ((ent[1] - team_value[1]) / year_average)
                        #                         + standard_deviation[str(key)]])
                        final_data[year].append([ent[0], ((ent[1] - team_value[1])
                                                          - (average_deviation - standard_deviation[str(year)]))])
    write_to_file(final_data, greater_than, field)
    total_time = time_converter(time.time() - start_time)
    logger.log("\tTime = " + total_time + '\n\n')


def write_to_file(final_data, greater_than, field):
    db = DatabaseConnection()
    counter = 0
    while len(final_data) > 0:
        target = None
        target_year = None
        for a, b in final_data.items():
            if target is not None:
                if greater_than:
                    if b[0][1] > target[1]:
                        target = b[0]
                        target_year = a
                    else:
                        continue
                else:
                    if b[0][1] < target[1]:
                        target = b[0]
                        target_year = a
                    else:
                        continue
            else:
                target = b[0]
                target_year = a
        if db.read('select league from team_years where teamId = "' + translate_team_id(target[0], target_year)
                   + '" and year = ' + str(target_year) + ';')[0][0].upper() in ['AL', 'NL']:
            counter += 1
            db.write('update team_years set ' + field + ' = ' + str(counter) + ' where teamId = "'
                     + translate_team_id(target[0], target_year) + '" and year = ' + str(target_year) + ';')
        del final_data[target_year][0]
        if len(final_data[target_year]) == 0:
            del final_data[target_year]
        else:
            continue
    db.close()


# team_ranker_ovr(2018, Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\"
#                              "dump.log"))
