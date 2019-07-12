from utilities.database.wrappers.baseball_data_connection import DatabaseConnection
from utilities.num_to_word import num_to_word


def consolidate_player_positions(ty_uid):
    db = DatabaseConnection(sandbox_mode=True)
    players_positions = db.read('select playerId, positions from player_positions where ty_uniqueidentifier = '
                                + str(ty_uid) + ';')
    db.close()
    roster = {}
    for player in players_positions:
        roster[player[0]] = player[1].split(',')
    return stringify_player_positions(roster)


def consolidate_hitter_spots(ty_uid):
    db = DatabaseConnection(sandbox_mode=True)
    batting_spots = {}
    query_fields = ''
    for num in range(9):
        query_fields += num_to_word(num + 1) + ', '
    for match_up in ['vr', 'vl']:
        for data in db.read('select playerId, ' + query_fields[:-2] + ' from hitter_spots where ty_uniqueidentifier = '
                            + str(ty_uid) + ' and matchup = "' + match_up + '";'):
            player_id = data[0]
            if player_id not in batting_spots:
                batting_spots[player_id] = {}
            batting_spots[player_id][match_up] = {}
            spot = 1
            for ent in data[1:]:
                batting_spots[player_id][match_up][spot] = ent
                spot += 1
    db.close()
    return stringify_hitter_spots(ensure_both_match_ups_are_present(batting_spots))


def ensure_both_match_ups_are_present(batting_spots):
    def none_for_all():
        temp_spots = {}
        for spot in range(9):
            temp_spots[spot+1] = None
        return temp_spots
    for player_id, match_ups in batting_spots.items():
        if len(match_ups) < 2:
            if 'vl' not in match_ups:
                batting_spots[player_id]['vl'] = none_for_all()
            else:
                batting_spots[player_id]['vr'] = none_for_all()
    return batting_spots


def write_roster_info(ty_uid, info):
    print(format_data(info))
    # db = DatabaseConnection(sandbox_mode=True)
    # db.write('update team_years set team_info = "' + format_data(info) + '" where ty_uniqueidentifier = ' + str(ty_uid)
    #          + ';')
    # db.close()


def format_data(info):
    this_string = ''
    players_dict = {}
    for data_type, string_dicts in info.items():
        for player_id, stringified_data in string_dicts.items():
            if player_id not in players_dict:
                players_dict[player_id] = stringified_data + '|'
            else:
                players_dict[player_id] += stringified_data + '|'
    for player_id, player_data in players_dict.items():
        this_string += player_id + '*' + player_data[:-1] + '&'
    return this_string[:-1]


def stringify_hitter_spots(hitter_spots):
    final_spots = {}
    for player_id, info in hitter_spots.items():
        temp_string = ''
        for match_up, spots in info.items():
            temp_string += match_up + '#'
            for spot, number in spots.items():
                temp_string += str(spot) + ':' + str(number) + ','
            temp_string = temp_string[:-1] + '+'
        final_spots[player_id] = temp_string[:-1]
    return final_spots


def stringify_player_positions(roster):
    stingified_roster = {}
    for player_id, positions in roster.items():
        stingified_positions = ''
        for position in positions:
            stingified_positions += position + ','
        stingified_roster[player_id] = stingified_positions[:-1]
    return stingified_roster


# print(consolidate_player_positions(2))
# print(consolidate_hitter_spots(2))
