from model.Game import Game
from controller.inning import simulate_inning


def simulate_game(away_team, home_team):
    game = Game(away_team, home_team)
    while game.get_inning() <= 9 or game.away_score == game.home_score:
        simulate_inning(game)
        game.increment_inning()
