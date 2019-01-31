from model.Game import Game
from controller.inning import inning


def simulate_game(away_team, home_team):
    game = Game(away_team, home_team)
    while game.get_inning() <= 9 or game.away_score == game.home_score:
        if game.get_inning() == 9 and game.get_half_inning() == 'bottom' and game.home_score > game.away_score:
            break
        else:
            inning(game)
