from utilities.database.wrappers.baseball_data_connection import DatabaseConnection
from utilities.properties import sandbox_mode
from utilities.num_to_word import num_to_word


def consolidate_player_positions(ty_uid):
    db = DatabaseConnection(sandbox_mode)
    roster = {}
    for player in db.read('select playerId, positions from player_positions where ty_uniqueidentifier = ' + str(ty_uid)
                          + ';'):
        roster[player[0]] = player[1].split(',')
    db.close()
    return roster


def consolidate_hitter_spots(ty_uid):
    db = DatabaseConnection(sandbox_mode)
    batting_spots = {}
    query_fields = ''
    for num in range(9):
        query_fields += num_to_word(num + 1) + ', '
    for data in db.read('select playerId, ' + query_fields[:-2] + ' from hitter_spots where ty_uniqueidentifier = '
                        + str(ty_uid) + ';'):
        batting_spots[data[0]] = {}
        spot = 1
        for ent in data[1:]:
            batting_spots[data[0]][spot] = ent
            spot += 1
    db.close()
    return batting_spots


def write_roster_info(ty_uid, info):
    db = DatabaseConnection(sandbox_mode)
    this_string = ''
    for player_id_1, spots in info['hitter_spots'].items():
        this_string += player_id_1 + '-'
        for player_id_2, positions in info['player_positions'].items():
            if player_id_1 == player_id_2:
                for place, total in spots.items():
                    this_string += str(place) + ':' + str(total) + ','
                this_string += '|'
                for position in positions:
                    this_string += position + ','
                this_string += '&'
                break
    db.write('update team_years set team_info = "' + this_string + '" where ty_uniqueidentifier = ' + str(ty_uid) + ';')
    db.close()


# print(consolidate_player_positions(62))
