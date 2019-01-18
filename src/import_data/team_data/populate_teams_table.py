from utilities.DB_Connect import DB_Connect
from utilities.translate_team_name import translate_team_name
from utilities.Logger import Logger
from utilities.time_converter import time_converter
import datetime
import time

logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\populate_teams_table."
                "log")


def populate_teams_table(year, driver_logger):
    driver_logger.log('\tPopulating teams table')
    print("Populating teams table")
    start_time = time.time()
    logger.log('Begin populating teams table for ' + str(year) + ' || Timestamp: ' + datetime.datetime.today()\
               .strftime('%Y-%m-%d %H:%M:%S'))
    with open('C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\background\\yearTeams.txt', 'rt') as file:
        db, cursor = DB_Connect.grab("baseballData")
        for line in file:
            if str(year) in line:
                temp_line = line.split(',')[1:-1]
                for team in temp_line:
                    team_id = team.split(';')[0]
                    DB_Connect.write(db, cursor, 'insert into teams (teamId, teamName) values ("' + team_id + '", "'
                                                 + translate_team_name(team_id).replace("'", "\'") + '");')
                break
    DB_Connect.close(db)
    total_time = time.time() - start_time
    logger.log('Populating teams table completed: ' + time_converter(total_time))
    driver_logger.log('\t\tTime = ' + time_converter(total_time))


# dump_logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\dump.log")
# populate_teams_table(2018, dump_logger)
