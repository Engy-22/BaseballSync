from utilities.database.wrappers.baseball_data_connection import DatabaseConnection
from utilities.translate_team_name import translate_team_name
from utilities.logger import Logger
from utilities.time_converter import time_converter
import datetime
import time
import os
from utilities.properties import sandbox_mode, import_driver_logger as driver_logger

logger = Logger(os.path.join("..", "..", "baseball-sync", "logs", "import_data", "populate_teams_table.log"))


def populate_teams_table(year):
    driver_logger.log('\tPopulating teams table')
    print("Populating teams table")
    start_time = time.time()
    logger.log('Begin populating teams table for ' + str(year) + ' || Timestamp: ' + datetime.datetime.today()\
               .strftime('%Y-%m-%d %H:%M:%S'))
    with open(os.path.join("..", "background", "yearTeams.txt"), 'rt') as file:
        db = DatabaseConnection(sandbox_mode)
        for line in file:
            if str(year) in line:
                temp_line = line.split(',')[1:-1]
                for team in temp_line:
                    team_id = team.split(';')[0]
                    db.write('insert into teams (teamId, teamName) values ("' + team_id + '", "'
                             + translate_team_name(team_id).replace("'", "\'") + '");')
                break
    db.close()
    total_time = time.time() - start_time
    logger.log('Populating teams table completed: ' + time_converter(total_time))
    driver_logger.log('\t\tTime = ' + time_converter(total_time))


# dump_logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\dump.log")
# populate_teams_table(2018, dump_logger)
