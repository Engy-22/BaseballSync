from utilities.database.wrappers.baseball_data_connection import DatabaseConnection
from utilities.properties import sandbox_mode
from model.players.player import Player


def get_starting_pitcher(team_id, year, game_number):
    def get_game(game_num, season_games):
        while game_num > season_games:
            game_num -= season_games
        return game_num
    db = DatabaseConnection(sandbox_mode)
    wins_losses = db.read('select wins, loses from team_years where teamid = "' + team_id + '" and year = '
                          + str(year) + ';')
    pitcher = db.read('select playerid from starting_pitchers where ty_uniqueidentifier = (select ty_uniqueidentifier'
                      ' from team_years where teamid = "' + team_id + '" and year = ' + str(year) + ') and gameNum = '
                      + str(get_game(game_number, int(wins_losses[0][0]) + int(wins_losses[0][1]))) + ';')[0][0]
    db.close()
    return Player(pitcher, team_id, year)


# print(get_starting_pitcher('TEX', 2016, 5))
