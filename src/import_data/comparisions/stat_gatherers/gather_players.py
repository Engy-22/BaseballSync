from utilities.DB_Connect import DB_Connect


def gather_players(year, player_type):
    db, cursor = DB_Connect.grab("baseballData")
    player_list = list(DB_Connect.read(cursor, "select playerId from player_teams, player_" + player_type + " where"
                                               + " player_" + player_type + ".PT_uniqueidentifier = "
                                               + " player_teams.PT_uniqueidentifier and player_" + player_type
                                               + ".year = " + str(year) + ";"))
    DB_Connect.close(db)
    return [player[0] for player in player_list]
