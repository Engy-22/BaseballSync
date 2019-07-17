import os
from utilities.database.wrappers.baseball_data_connection import DatabaseConnection
from import_data.consolidata.team_roster_info import consolidate_hitter_spots, consolidate_player_positions
from import_data.consolidata.write_consolidated_data import write_roster_info
from import_data.consolidata.player_stats import consolidate_player_stats
from utilities.logger import Logger
from utilities.properties import sandbox_mode, log_prefix, import_driver_logger as driver_logger
from utilities.time_converter import time_converter
import datetime
import time

logger = Logger(os.path.join(log_prefix, "import_data", "consolidata.log"))


def consolidate_data(year):
    driver_logger.log("\tConsolidating data")
    print("Consolidating data")
    start_time = time.time()
    logger.log("Consolidating team data || Timestamp: " + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
    db = DatabaseConnection(sandbox_mode)
    for ty_uid in db.read('select ty_uniqueidentifier from team_years where year = ' + str(year) + ';'):
        team_start_time = time.time()
        logger.log('\t' + db.read('select teamId from team_years where ty_uniqueidentifier = ' + str(ty_uid[0])
                                  + ';')[0][0])
        write_roster_info(ty_uid[0], {'hitter_spots': consolidate_hitter_spots(ty_uid[0]),
                                      'player_positions': consolidate_player_positions(ty_uid[0]),
                                      'batter_stats': consolidate_player_stats(ty_uid[0], 'batting', year),
                                      'pitcher_stats': consolidate_player_stats(ty_uid[0], 'pitching', year),
                                      'fielder_stats': consolidate_player_stats(ty_uid[0], 'fielding', year)})
        logger.log('\t\tTime = ' + time_converter(time.time() - team_start_time))
    db.close()
    total_time = time_converter(time.time() - start_time)
    logger.log("Done consolidating team data: Time = " + total_time)
    driver_logger.log("\t\tTime = " + total_time)


# consolidate_data(2017)
ty_uid = 11
temp_year = 2017
write_roster_info(ty_uid, {'hitter_spots': consolidate_hitter_spots(ty_uid),
                           'player_positions': consolidate_player_positions(ty_uid),
                           'batter_stats': consolidate_player_stats(ty_uid, 'batting', temp_year),
                           'pitcher_stats': consolidate_player_stats(ty_uid, 'pitching', temp_year),
                           'fielder_stats': consolidate_player_stats(ty_uid, 'fielding', temp_year)})
