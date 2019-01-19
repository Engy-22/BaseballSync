from utilities.dbconnect import DatabaseConnection


def gather_players(year, player_type, all, driver_logger):
    db = DatabaseConnection()
    query = "select playerId from player_teams, player_" + player_type + " where player_" + player_type\
            + ".PT_uniqueidentifier = player_teams.PT_uniqueidentifier and player_" + player_type + ".year = "\
            + str(year)
    if all:
        player_list = list(db.read(query + ";"))
    else:
        player_list = list(db.read(query + " and certainty < 1.0;"))
    db.close()
    return [player[0] for player in player_list]
