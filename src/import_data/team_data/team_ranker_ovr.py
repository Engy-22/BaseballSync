import time
from utilities.database.wrappers.baseball_data_connection import DatabaseConnection
from utilities.translate_team_id import translate_team_id
from utilities.time_converter import time_converter
from utilities.logger import Logger
from utilities.properties import sandbox_mode


def team_ranker_ovr(data, greater_than, field, all_time_rpg, standard_deviation, average_deviation, playoff_data=None):
    logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\"
                    "team_ranker_ovr.log")
    logger.log("Calculating overall team ranks: " + field)
    start_time = time.time()
    final_data = {}
    if field != "ovrRank_ovr":
        for year, value in data.items():
            final_data[year] = []
            for ent in value:
                if field == "offRank_ovr":
                    final_data[year].append([ent[0], (ent[1]/all_time_rpg) /
                                             (standard_deviation[str(year)]/average_deviation)])
                else:
                    final_data[year].append([ent[0], (ent[1]/all_time_rpg) *
                                             (standard_deviation[str(year)]/average_deviation)])
    else:
        for year, value in data.items():
            final_data[year] = []
            for ent in value:
                for team_value in data[year]:
                    if team_value[0] == ent[0]:
                        playoff_bump = 1.0
                        for accomplishment, team_id in playoff_data.items():
                            if accomplishment == 'ws_champ':
                                playoff_bump += 0.05
                            playoff_bump += 0.05
                        final_data[year].append([ent[0], (ent[1]/(standard_deviation[str(year)]/average_deviation)) *
                                                 playoff_bump])
    write_to_file(final_data, greater_than, field)
    total_time = time_converter(time.time() - start_time)
    logger.log("\tTime = " + total_time + '\n\n')


def write_to_file(final_data, greater_than, field):
    db = DatabaseConnection(sandbox_mode)
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
