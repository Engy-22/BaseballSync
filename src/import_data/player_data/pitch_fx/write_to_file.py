from utilities.dbconnect import DatabaseConnection


def write_to_file(player_type, player, year, matchup, count, pitch_type, ball_strike, swing_take, outcome, trajectory,
                  field, direction):
    db = DatabaseConnection()
    db.write('insert into ' + player_type + '_pitches (pitchid, playerid, year, matchup, count, pitch_type, swing_take,'
             ' ball_strike, outcome, trajectory, field, direction) values (default, ' + str(player) + ', ' + str(year)
             + ', "' + matchup + '", "' + str(count) + '", "' + pitch_type + '", "' + swing_take + '", "' + ball_strike
             + '", "' + outcome + '", "' + trajectory + '", "' + field + '", "' + direction + '");')
    db.close()
