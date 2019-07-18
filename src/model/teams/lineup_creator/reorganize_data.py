def reorganize_batting_spots(roster, team_info, spot, opposing_pitcher_handedness):
    options = {}
    for player in roster:
        try:
            if not player.get_batting_spots():
                player.set_batting_spots(team_info['hitter_spots'][player.get_player_id()])
            starts = player.get_batting_spots()['v' + opposing_pitcher_handedness.lower()][spot]
        except KeyError:
            starts = 0
        if starts > 0:
            options[player] = starts
    return options
