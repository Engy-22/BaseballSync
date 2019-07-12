def reorganize_batting_spots(roster, spot, opposing_pitcher_handedness):
    options = {}
    for player in roster:
        try:
            starts = player.get_batting_spots()['v' + opposing_pitcher_handedness.lower()][spot]
        except KeyError:
            starts = 0
        if starts > 0:
            options[player] = starts
    return options
