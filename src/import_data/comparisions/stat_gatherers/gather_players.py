from utilities.database.wrappers.baseball_data_connection import DatabaseConnection
from utilities.properties import sandbox_mode


def gather_players(year, player_type, gather_all, logger):
    db = DatabaseConnection(sandbox_mode)
    query = "select playerId from player_teams, player_" + player_type + " where player_" + player_type + ".PT_unique"\
            "identifier = player_teams.PT_uniqueidentifier and player_" + player_type + ".year = " + str(year)
    if gather_all:
        logger.log('\t\t\tGathering all players')
        player_list = list(db.read(query + ";"))
    else:
        logger.log('\t\t\tGathering players that need comps')
        player_list = list(db.read(query + " and certainty < 1.0 and certainty > 0.0;"))
    db.close()
    return [player[0] for player in player_list]


# for year in range(1996, 2009, 1):
#     print(str(year) + ': ' + str(len(gather_players(year, 'batting', False))))
