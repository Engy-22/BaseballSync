from model.teams.lineup_creator.get_starting_pitcher import get_starting_pitcher


def create_lineup(team_id, year, roster, batting_spots, game_num):
    starting_pitcher = get_starting_pitcher(team_id, year, game_num)
    for place in range(9):
        pass


# create_lineup("CLE", 2016, {}, {}, 25)
