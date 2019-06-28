def player_available(order, batter):
    for player in order:
        if player.get_player_id() == batter.get_player_id():
            return False
    return True


def position_available(position_list, position):
    for this_position in position_list:
        if this_position == position:
            return False
    return True


def sp_can_bat_here(pitcher, place, opposing_pitcher_handedness):
    if pitcher.get_batting_spots()[opposing_pitcher_handedness][place] == 0:
        print('caught')
    return pitcher.get_batting_spots()[opposing_pitcher_handedness][place] > 0
