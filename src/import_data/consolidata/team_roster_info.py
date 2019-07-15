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
                if ent:
                    batting_spots[player_id][match_up][spot] = ent
                spot += 1
    db.close()
    return ensure_both_match_ups_are_present(batting_spots)


def ensure_both_match_ups_are_present(batting_spots):
    for player_id, match_ups in batting_spots.items():
        if len(match_ups) < 2:
            if 'vl' not in match_ups:
                batting_spots[player_id]['vl'] = {}
            else:
                batting_spots[player_id]['vr'] = {}
    return batting_spots


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
