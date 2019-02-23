def reorganize_batting_spots(roster, spot):
    options = {}
    for player in roster:
        starts = player.get_batting_spots()[spot]
        if starts > 0:
            options[player] = starts
    return options
