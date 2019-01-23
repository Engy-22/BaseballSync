from utilities.dbconnect import DatabaseConnection


def write_to_file(pitch_type, swing_take, meta_data):
    db = DatabaseConnection()
    db.write()
    db.close()
