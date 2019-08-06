from utilities.database.wrappers.baseball_data_connection import DatabaseConnection


def write_roster_info(ty_uid, info):
    # print(info['pitcher_stats']['klubeco01']['advanced_pitching_stats']['overall_pitch_location_pitching'])
    db = DatabaseConnection(sandbox_mode=True)
    db.write('update team_years set team_info = "' + str(info) + '" where ty_uniqueidentifier = ' + str(ty_uid) + ';')
    db.close()
