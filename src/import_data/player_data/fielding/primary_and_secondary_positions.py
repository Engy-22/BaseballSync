import time
import datetime
from utilities.baseball_data_connection import DatabaseConnection
from utilities.logger import Logger
from utilities.time_converter import time_converter

logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\"
                "primary_and_secondary_positions.log")


def primary_and_secondary_positions(year, driver_logger):
    print("adding primary and secondary positions")
    driver_logger.log("\tAdding primary and secondary positions")
    start_time = time.time()
    logger.log("Downloading " + str(year) + " primary and secondary data || Timestamp: " + datetime.datetime.today()\
               .strftime('%Y-%m-%d %H:%M:%S'))
    db = DatabaseConnection()
    logger.log("\tAssembling list of players")
    assembly_time = time.time()
    teams_from_year = db.read("select TY_uniqueidentifier from team_years where year=" + str(year)+';')
    teams_from_year_range = db.read("select TY_uniqueidentifier from team_years where year between "
                                                    + str(year-25) + ' and ' + str(year) + ';')
    player_positions = []
    player_positions_range = []
    for team in teams_from_year:
        player_positions += db.read('select playerId, positions from player_positions where '
                                                    + 'TY_uniqueidentifier = ' + str(team[0]) + ';')
    for team in teams_from_year_range:
        player_positions_range += db.read('select playerId, positions from player_positions where '
                                                          + 'TY_uniqueidentifier = ' + str(team[0]) + ';')
    logger.log("\t\tTime = " + time_converter(time.time() - assembly_time))
    logger.log("\tDetermining positions")
    determination_time = time.time()
    for player in player_positions:
        player_position_string = get_player_positions(player, player_positions_range)
        player_positions_dict = determine_primary_position(player_position_string)
        write_to_file(player[0].replace("'", "\'"), player_positions_dict)
    db.close()
    logger.log("\t\tTime = " + time_converter(time.time() - determination_time))
    total_time = time_converter(time.time() - start_time)
    logger.log("Done downloading primary and secondary positions: time = " + total_time + '\n\n')
    driver_logger.log("\t\tTime = " + total_time)


def write_to_file(player, positions_dict):
    db = DatabaseConnection()
    positions = sorted(positions_dict.items(), key=lambda kv: kv[1], reverse=True)
    secondary = positions[1:]
    secondary_positions = []
    for ent in secondary:
        secondary_positions.append(ent[0])
    if len(secondary_positions) == 0:
        db.write('update players set primaryPosition = "' + positions[0][0]+'", secondaryPositions' + ' = Null where '
                 'playerId = "' + player + '";')
    else:
        db.write('update players set primaryPosition = "' + positions[0][0]+'", secondaryPositions' + ' = "'
                 + ','.join(secondary_positions) + '" where playerId = "' + player + '";')
    db.close()


def determine_primary_position(position_string):
    position_list = position_string.split(';')
    position_dict = {}
    for p in position_list:
        temp_p = p.split(',')
        for i in range(len(temp_p)):
            if temp_p[i] not in position_dict:
                position_dict[temp_p[i]] = 0
            if i == 0:
                position_dict[temp_p[i]] += 2
            else:
                position_dict[temp_p[i]] += 1
    return position_dict


def get_player_positions(player, year_range):
    positions = []
    for ent in year_range:
        if player[0] == ent[0]:
            positions.append(ent[1])
    return ';'.join(positions)


# dump_logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\dump.log")
# primary_and_secondary_positions(2018, dunp_logger)
