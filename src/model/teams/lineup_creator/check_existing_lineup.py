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
