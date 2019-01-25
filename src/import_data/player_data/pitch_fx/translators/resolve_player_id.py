from utilities.dbconnect import DatabaseConnection


def resolve_player_id(player_num, player_name):
    db = DatabaseConnection()
    db.close()
