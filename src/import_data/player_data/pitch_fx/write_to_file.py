from utilities.connections.baseball_data_connection import DatabaseConnection


def write_to_file(players_url, player_type, player_id, team_id, year, matchup, count, pitch_type, ball_strike,
                  swing_take, outcome, trajectory, field, direction, original_player_id, sandbox_mode):
    if player_id is None:
        with open('C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\src\\import_data\\player_data\\pitch_fx'
                  '\\multiple_players.csv', 'a') as file:
            file.write(str(original_player_id) + ': --> insert into ' + player_type + '_pitches (pitchid, playerid, '
                       'year, matchup, count, pitch_type, swing_take, ball_strike, outcome, trajectory, field, '
                       'direction, P' + player_type[0] + '_uniqueidentifier) values (default, ' + str(player_id) + ', '
                       + str(year) + ', "' + matchup + '", "' + str(count) + '", "' + pitch_type + '", "' + swing_take
                       + '", "' + ball_strike + '", "' + outcome + '", "' + trajectory + '", "' + field + '", "'
                       + direction + '", (select P' + player_type[0] + '_uniqueidentifier from player_'
                       + player_type[:-2] + 'ing where year = ' + str(year) + ' and pt_uniqueidentifier = (select '
                       'pt_uniqueidentifier from player_teams where playerid = "' + str(player_id) + '" and teamid = "'
                       + team_id + '"))); -- ' + players_url + '\n')
    else:
        db = DatabaseConnection(sandbox_mode)
        db.write('insert into ' + player_type + '_pitches (pitchid, playerid, year, matchup, count, pitch_type, '
                 'swing_take, ball_strike, outcome, trajectory, field, direction, P' + player_type[0]
                 + '_uniqueidentifier) values (default, "' + player_id + '", ' + str(year) + ', "' + matchup + '", "'
                 + str(count) + '", "' + pitch_type + '", "' + swing_take + '", "' + ball_strike + '", "' + outcome
                 + '", "' + trajectory + '", "' + field + '", "' + direction + '", (select P' + player_type[0]
                 + '_uniqueidentifier from player_' + player_type[:-2] + 'ing where year = ' + str(year) + ' and '
                 'pt_uniqueidentifier = (select pt_uniqueidentifier from player_teams where playerid = "' + player_id
                 + '" and teamid = "' + team_id + '")));')
        db.close()


def write_pickoff(pitcher, team, year, base, attempts_successes, sandbox_mode):
    # print(base + ' ' + attempts_successes)
    # print(pitcher, team, year, base, attempts_successes)
    db = DatabaseConnection(sandbox_mode)
    pt_uid = db.read('select pt_uniqueidentifier from player_teams where playerid = "' + pitcher + '" and teamid = "'
                     + team + '";')[0][0]
    if db.read('select pickoff_' + base + '_' + attempts_successes + ' from player_pitching where '
               'pt_uniqueidentifier = ' + str(pt_uid) + ' and year = ' + str(year) + ';')[0][0] is None:
        db.write('update player_pitching set pickoff_' + base + '_' + attempts_successes + ' = 1 where '
                 'pt_uniqueidentifier = ' + str(pt_uid) + ' and year = ' + str(year) + ';')
    else:
        db.write('update player_pitching set pickoff_' + base + '_' + attempts_successes + ' = '
                 + str(int(db.read('select pickoff_' + base + '_' + attempts_successes + ' from player_pitching where '
                                   'pt_uniqueidentifier = ' + str(pt_uid) + ' and year = ' + str(year) + ';')[0][0])
                       + 1) + ' where pt_uniqueidentifier = ' + str(pt_uid) + ' and year = ' + str(year) + ';')
    db.close()
