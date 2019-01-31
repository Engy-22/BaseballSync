def simulate_inning(game):
    for half in ['top', 'bottom']:
        simulate_half_inning(game, half)


def simulate_half_inning(game, half):
    if game.get_inning() == 9 and half == 'bottom' and game.home_score > game.away_score:
        return

