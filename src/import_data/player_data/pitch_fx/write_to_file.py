import os
from utilities.database.wrappers.pitch_fx_connection import PitchFXDatabaseConnection
from utilities.database.wrappers.baseball_data_connection import DatabaseConnection
from utilities.properties import sandbox_mode


def write_to_file(innings_url, player_type, player_id, team_id, year, month, day, match_up, count, pitch_type,
                  ball_strike, swing_take, outcome, trajectory, field, direction, original_player_id, x_coord, y_coord,
                  velocity):
    if player_id is None:
        with open(os.path.join("..", "..", "baseball-sync", "src", "import_data", "player_data", "pitch_fx",
                               "multiple_players.csv"), 'a') as file:
            file.write(str(original_player_id) + ': --> insert into ' + player_type + '_pitches (pitchid, playerid, '
                       'year, matchup, count, pitch_type, swing_take, ball_strike, outcome, trajectory, field, '
                       'direction, P' + player_type[0] + '_uniqueidentifier) values (default, ' + str(player_id) + ', '
                       + str(year) + ', "' + match_up + '", "' + str(count) + '", "' + pitch_type + '", "' + swing_take
                       + '", "' + ball_strike + '", "' + outcome + '", "' + trajectory + '", "' + field + '", "'
                       + direction + '", (select P' + player_type[0] + '_uniqueidentifier from player_'
                       + player_type[:-2] + 'ing where year = ' + str(year) + ' and pt_uniqueidentifier = (select '
                       'pt_uniqueidentifier from player_teams where playerid = "' + str(player_id) + '" and teamid = "'
                       + team_id + '"))); -- ' + innings_url + '\n')
    else:
        db_to_read = DatabaseConnection(sandbox_mode=True)
        # if len(db_to_read.read('select p' + player_type[0] + '_uniqueidentifier from player_' + player_type + ' where '
        #                        'playerdId = ' + player_id + ' and year = ' + str(year) + ';')[0]) > 1:
        #     team_id = 'TOT'  <---this code would've made sure that all players that played for more than one team 
        p_uid = str(db_to_read.read('select P' + player_type[0] + '_uniqueidentifier from player_' + player_type[:-2]
                                    + 'ing where year = ' + str(year) + ' and pt_uniqueidentifier = (select '
                                    'pt_uniqueidentifier from player_teams where playerid = "' + player_id + '" and '
                                    'teamid = "' + team_id + '");')[0][0])
        db_to_read.close()
        db = PitchFXDatabaseConnection(sandbox_mode=True)
        if x_coord == 'None' or y_coord == 'None':
            if velocity == 'None':
                db.write('insert into ' + player_type + '_pitches (pitchid, playerid, year, month, day, matchup, count,'
                         ' pitch_type, swing_take, ball_strike, outcome, trajectory, field, direction, P'
                         + player_type[0] + '_uniqueidentifier) values (default, "' + player_id + '", ' + str(year)
                         + ', ' + month + ', ' + day + ', "' + match_up + '", "' + str(count) + '", "' + pitch_type
                         + '", "' + swing_take + '", "' + ball_strike + '", "' + outcome + '", "' + trajectory + '", "'
                         + field + '", "' + direction + '", ' + p_uid + ');')
            else:
                db.write('insert into ' + player_type + '_pitches (pitchid, playerid, year, month, day, matchup, count,'
                         ' pitch_type, swing_take, ball_strike, outcome, trajectory, field, direction, velocity, P'
                         + player_type[0] + '_uniqueidentifier) values (default, "' + player_id + '", ' + str(year)
                         + ', ' + month + ', ' + day + ', "' + match_up + '", "' + str(count) + '", "' + pitch_type
                         + '", "' + swing_take + '", "' + ball_strike + '", "' + outcome + '", "' + trajectory + '", "'
                         + field + '", "' + direction + '", ' + velocity + ', ' + p_uid + ');')
        else:
            if velocity == 'None':
                db.write('insert into ' + player_type + '_pitches (pitchid, playerid, year, month, day, matchup, count,'
                         ' pitch_type, swing_take, ball_strike, outcome, trajectory, field, direction, x, y, P'
                         + player_type[0] + '_uniqueidentifier) values (default, "' + player_id + '", ' + str(year)
                         + ', ' + month + ', ' + day + ', "' + match_up + '", "' + str(count) + '", "' + pitch_type
                         + '", "' + swing_take + '", "' + ball_strike + '", "' + outcome + '", "' + trajectory + '", "'
                         + field + '", "' + direction + '", ' + x_coord + ', ' + y_coord + ', ' + p_uid + ');')
            else:
                db.write('insert into ' + player_type + '_pitches (pitchid, playerid, year, month, day, matchup, count,'
                         ' pitch_type, swing_take, ball_strike, outcome, trajectory, field, direction, x, y, velocity,'
                         ' P' + player_type[0] + '_uniqueidentifier) values (default, "' + player_id + '", ' + str(year)
                         + ', ' + month + ', ' + day + ', "' + match_up + '", "' + str(count) + '", "' + pitch_type
                         + '", "' + swing_take + '", "' + ball_strike + '", "' + outcome + '", "' + trajectory + '", "'
                         + field + '", "' + direction + '", ' + x_coord + ', ' + y_coord + ', ' + velocity + ', '
                         + p_uid + ');')
        db.close()


def write_pickoff(pitcher, team_id, year, base, attempts_successes):
    db = DatabaseConnection(sandbox_mode=True)
    if len(db.read('select pp_uniqueidentifier from player_pitching where playerdId = ' + pitcher + ' and year = '
                   + str(year) + ';')[0]) > 1:
        team_id = 'TOT'
    pt_uid = db.read('select pt_uniqueidentifier from player_teams where playerid = "' + pitcher + '" and teamid = "'
                     + team_id + '";')[0][0]
    if db.read('select pickoff_' + base + '_' + attempts_successes + ' from player_pitching where pt_uniqueidentifier'
               ' = ' + str(pt_uid) + ' and year = ' + str(year) + ';')[0][0] is None:
        db.write('update player_pitching set pickoff_' + base + '_' + attempts_successes + ' = 1 where '
                 'pt_uniqueidentifier = ' + str(pt_uid) + ' and year = ' + str(year) + ';')
    else:
        db.write('update player_pitching set pickoff_' + base + '_' + attempts_successes + ' = '
                 + str(int(db.read('select pickoff_' + base + '_' + attempts_successes + ' from player_pitching where '
                 'pt_uniqueidentifier = ' + str(pt_uid) + ' and year = ' + str(year) + ';')[0][0]) + 1)
                 + ' where pt_uniqueidentifier = ' + str(pt_uid) + ' and year = ' + str(year) + ';')
    db.close()
