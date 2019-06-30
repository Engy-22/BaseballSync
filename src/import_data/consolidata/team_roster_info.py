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
    return roster


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
    return ensure_both_match_ups_are_present(batting_spots)


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
    db = DatabaseConnection(sandbox_mode=True)
    this_string = ''
    for player_id, match_ups in info['hitter_spots'].items():
        this_string += player_id + '-'
        for player_id_reference, positions in info['player_positions'].items():
            if player_id == player_id_reference:
                for static_match_up in ['vr', 'vl']:
                    this_string += static_match_up + '#'
                    for place, total in match_ups[static_match_up].items():
                        this_string += str(place) + ':' + str(total) + ','
                this_string += '|'
                for position in positions:
                    this_string += position + ','
                this_string += '&'
                break
    db.write('update team_years set team_info = "' + this_string + '" where ty_uniqueidentifier = ' + str(ty_uid) + ';')
    db.close()


# print(consolidate_player_positions(2))
# print(consolidate_hitter_spots(2))
