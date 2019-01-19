import time
import datetime
from utilities.dbconnect import DatabaseConnection
from utilities.time_converter import time_converter
from utilities.Logger import Logger

logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\team_certainties.log")


def team_certainties(year, driver_logger):
    print('aggregating team statistic certainties')
    driver_logger.log("\tAggregating team statistic certainties")
    start_time = time.time()
    logger.log("Calculating team certainties || Timestamp: " + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
    db = DatabaseConnection()
    stat_types = ["batting", "pitching"]
    for stat_type in stat_types:
        ty_uids = db.read('select ty_uniqueidentifier, teamid from team_years where year = ' + str(year))
        for ty_uid in ty_uids:
            pau = 0
            player_list = list(db.read('select playerid from player_positions where ty_uniqueidentifier = '
                                       + str(ty_uid[0]) + ';'))
            for player in player_list:
                pt_uid = db.read('select pt_uniqueidentifier from player_teams where playerid = "' + player[0] + '" and'
                                 ' teamid = "' + ty_uid[1] + '";')[0][0]
                try:
                    ent = db.read('select pa, certainty from player_' + stat_type + ' where year = ' + str(year)
                                  + ' and pt_uniqueidentifier = ' + str(pt_uid) + ';')
                    pau += int(ent[0][0]) - (int(ent[0][0]) * float(ent[0][1]))
                except IndexError:
                    continue
                except TypeError:
                    continue
            pa = int(db.read('select pa from team_years where ty_uniqueidentifier = ' + str(ty_uid[0]) + ';')[0][0])
            db.write('update team_years set certainty = ' + str((pa - pau) / pa) + ' where ty_uniqueidentifier = '
                     + str(ty_uid[0]) + ';')
    db.close()
    total_time = time_converter(time.time() - start_time)
    logger.log("Done calculating team certainties: time = " + total_time + '\n\n')
    driver_logger.log("\t\tTime = " + total_time)


# dump_logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\dump.log")
# team_certainties(2018, dump_logger)
