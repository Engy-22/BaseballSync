import os
from utilities.database.wrappers.baseball_data_connection import DatabaseConnection
from utilities.database.wrappers.pitch_fx_connection import PitchFXDatabaseConnection
from utilities.logger import Logger
import time
import datetime
from concurrent.futures import ThreadPoolExecutor
from utilities.time_converter import time_converter
from utilities.properties import log_prefix, import_driver_logger as driver_logger

logger = Logger(os.path.join(log_prefix, "import_data", "aggregate_pitch_fx.log"))


def aggregate_pitch_fx(year, month=None, day=None):
    print('Aggregating pitch fx data')
    driver_logger.log("\tAggregating pitch fx data")
    start_time = time.time()
    logger.log("Aggregating pitch fx data for " + str(year) + ' || Timestamp: ' + datetime.datetime.today().
               strftime('%Y-%m-%d %H:%M:%S'))
    change_multi_team_players_uids(year, month, day, 'pitching')
    change_multi_team_players_uids(year, month, day, 'batting')
    aggregate_and_write(year, month, day, 'pitching')
    aggregate_and_write(year, month, day, 'batting')
    total_time = time_converter(time.time() - start_time)
    logger.log("Done aggregating " + str(year) + " pitch fx data: Time = " + total_time + '\n\n')
    driver_logger.log("\t\tTime = " + total_time)


def aggregate_and_write(year, month, day, player_type):
    start_time = time.time()
    driver_logger.log("\t\tAggregating " + player_type + " data")
    logger.log("\tAggregating " + player_type + " data and writing to database")
    for player_id in get_players(year, month, day, player_type):
        aggregate(year, month, day, player_id[0], player_type)
        # break
    total_time = time_converter(time.time()-start_time)
    logger.log("\tDone aggregating and writing " + player_type + " data: Time = " + total_time)
    driver_logger.log("\t\t\tTime = " + total_time)


def aggregate(year, month, day, player_id, player_type):
    logger.log('\t' + player_id)
    start_time = time.time()
    match_ups = ['vr', 'vl']
    pitch_usage = {}
    pitch_type_outcomes = {}
    swing_rates = {}
    strike_percents = {}
    trajectories = {}
    fields = {}
    directions = {}
    trajectories_by_outcome = {}
    fields_by_outcome = {}
    directions_by_outcome = {}
    p_uid = get_player_unique_stat_id(year, player_type, player_id)
    pitches = get_player_pitches(p_uid, year, month, day, player_type)
    for match_up in match_ups:
        aggregate_hbp(player_id, year, match_up, p_uid, player_type)
        pitch_usage[match_up] = {}
        pitch_type_outcomes[match_up] = {}
        swing_rates[match_up] = {}
        strike_percents[match_up] = {}
        trajectories[match_up] = {}
        fields[match_up] = {}
        directions[match_up] = {}
        trajectories_by_outcome[match_up] = {}
        fields_by_outcome[match_up] = {}
        directions_by_outcome[match_up] = {}
        for ball in range(4):
            for strike in range(3):
                count = str(ball) + '-' + str(strike)
                pitch_usage[match_up][count] = {}
                pitch_type_outcomes[match_up][count] = {}
                swing_rates[match_up][count] = {}
                strike_percents[match_up][count] = {}
                trajectories[match_up][count] = {}
                fields[match_up][count] = {}
                directions[match_up][count] = {}
                for pitch_type, pitch_type_list in sort_by_pitch_type(pitches, match_up, ball, strike).items():
                    pitch_usage[match_up][count][pitch_type] = len(pitch_type_list)
                    pitch_type_outcomes[match_up][count][pitch_type] = sort_further_by_outcome(pitch_type_list)
                    swing_rates[match_up][count][pitch_type] = calculate_swing_rate(pitch_type_list)
                    strike_percents[match_up][count][pitch_type] = calculate_strike_percent(pitch_type_list)
                    trajectories[match_up][count][pitch_type] = sort_further_by_trajectory(pitch_type_list)
                    fields[match_up][count][pitch_type] = sort_further_by_field(pitch_type_list)
                    directions[match_up][count][pitch_type] = sort_further_by_direction(pitch_type_list)
                write_pitch_usage(player_id, p_uid, year, match_up, count, pitch_usage[match_up][count], player_type)
                write_swing_rate(player_id, p_uid, year, match_up, count, pitch_usage[match_up][count],
                                 swing_rates[match_up][count], player_type)
                write_strike_percent(player_id, p_uid, year, match_up, count, pitch_usage[match_up][count],
                                     strike_percents[match_up][count], player_type)
                write_outcomes(player_id, p_uid, year, match_up, count, pitch_type_outcomes[match_up][count],
                               player_type)
                write_trajectory_by_pitch_type(player_id, p_uid, year, match_up, count, trajectories[match_up][count],
                                               player_type)
                write_field_by_pitch_type(player_id, p_uid, year, match_up, count, fields[match_up][count], player_type)
                write_direction_by_pitch_type(player_id, p_uid, year, match_up, count, directions[match_up][count],
                                              player_type)
        for outcome, truncated_pitches in sort_pitches_by_outcome(pitches).items():
            trajectories_by_outcome[match_up][outcome] = calculate_trajectory_by_outome(truncated_pitches)
            fields_by_outcome[match_up][outcome] = calculate_field_by_outcome(truncated_pitches)
            directions_by_outcome[match_up][outcome] = calculate_direction_by_outcome(truncated_pitches)
        write_trajectory_by_outcome(player_id, p_uid, year, match_up, trajectories_by_outcome[match_up], player_type)
        write_field_by_outcome(player_id, p_uid, year, match_up, fields_by_outcome[match_up], player_type)
        write_direction_by_outcome(player_id, p_uid, year, match_up, directions_by_outcome[match_up], player_type)
    logger.log('\t\tTime = ' + time_converter(time.time()-start_time))


def get_player_unique_stat_id(year, player_type, player_id):
    temp_p_uid = []
    db = DatabaseConnection(sandbox_mode=True)
    for pt_uid in db.read('select pt_uniqueidentifier from player_teams where playerid = "' + player_id + '";'):
        try:
            temp_p_uid.append(db.read('select p' + player_type[0] + '_uniqueidentifier from player_' + player_type
                                      + ' where year = ' + str(year) + ' and pt_uniqueidentifier = ' + str(pt_uid[0])
                                      + ';')[0][0])
        except IndexError:
            pass
    if len(temp_p_uid) > 1:
        p_uid = db.read('select p' + player_type[0] + '_uniqueidentifier from player_' + player_type + ' where year = '
                        + str(year) + ' and pt_uniqueidentifier = (select pt_uniqueidentifier from player_teams where '
                        'playerid = "' + player_id + '" and teamid = "TOT")' + ';')[0][0]
    else:
        p_uid = temp_p_uid[0]
    db.close()
    return p_uid


def get_player_pitches(p_uid, year, month, day, player_type):
    table = player_type[:-3] + 'er_pitches'
    extended_query = ''
    if month is not None and day is not None:
        extended_query += ' and month = ' + str(month) + ' and day = ' + str(day)
    pitch_fx_db = PitchFXDatabaseConnection(sandbox_mode=True)
    pitches = pitch_fx_db.read('select * from ' + table + ' where p' + player_type[0] + '_uniqueidentifier = '
                               + str(p_uid) + ' and year = ' + str(year) + extended_query + ';')
    pitch_fx_db.close()
    return pitches


def get_players(year, month, day, player_type):
    logger.log('\tGathering players')
    start_time = time.time()
    db = PitchFXDatabaseConnection(sandbox_mode=True)
    extended_query = ''
    if month is not None and day is not None:
        extended_query += ' and month = ' + str(month) + ' and day = ' + str(day)
    players = set(db.read('select playerid from ' + player_type[:-3] + 'er_pitches where year = ' + str(year)
                          + extended_query + ';'))
    db.close()
    logger.log('\t\tTime = ' + time_converter(time.time()-start_time))
    return players


def aggregate_hbp(player_id, year, match_up, p_uid, player_type):
    pitches = {}
    db = PitchFXDatabaseConnection(sandbox_mode=True)
    vs = {'b': 'p', 'p': 'b'}
    hbps = db.read('select pitch_type, count(*) from ' + player_type[:-3] + 'er_pitches where year = ' + str(year)
                   + ' and playerid = "' + player_id + '" and matchup = "' + match_up + 'h' + vs[player_type[0]]
                   + '" and outcome = "hbp" group by  pitch_type;')
    for pitch_type in db.read('select pitch_type, count(*) from ' + player_type[:-3] + 'er_pitches where year = '
                              + str(year) + ' and playerid = "' + player_id + '" and matchup="' + match_up
                              + 'h' + vs[player_type[0]] + '" group by pitch_type;'):
        pitches[pitch_type[0]] = pitch_type[1]
    db.close()
    new_db = DatabaseConnection(sandbox_mode=True)
    if len(new_db.read('select hbp_id from hbp_' + player_type + ' where playerid = "' + player_id + '" and year = '
                       + str(year) + ' and matchup = "' + match_up + '";')) == 0:
        fields = ''
        values = ''
        for hbp_by_pitch_type in hbps:
            fields += ', ' + hbp_by_pitch_type[0]
            values += ', ' + str(round(hbp_by_pitch_type[1]/pitches[hbp_by_pitch_type[0]], 3))
        new_db.write('insert into hbp_' + player_type + ' (hbp_id, playerid, year, matchup' + fields + ', p_uid)'
                     ' values (default, "' + player_id + '", ' + str(year) + ', "' + match_up + '"' + values + ', '
                     + str(p_uid) + ');')
    else:
        sets = ''
        for hbp_by_pitch_type in hbps:
            sets += hbp_by_pitch_type[0] + '=' + str(round(hbp_by_pitch_type[1]/pitches[hbp_by_pitch_type[0]], 3)) + ','
        if len(sets) > 0:
            new_db.write('update hbp_' + player_type + ' set ' + sets[:-1] + 'where playerid = "' + player_id + '" and'
                         ' year = ' + str(year) + ' and matchup = "' + match_up + '";')
    new_db.close()


def write_pitch_usage(player_id, p_uid, year, match_up, count, pitch_type_dict, player_type):
    db = DatabaseConnection(sandbox_mode=True)
    total_pitches = 0
    fields = ''
    values = ''
    for pitch_type, total in pitch_type_dict.items():
        total_pitches += total
    with ThreadPoolExecutor(os.cpu_count()) as executor1:
        if len(db.read('select uid from pitch_usage_' + player_type + ' where playerid = "' + player_id + '" and '
                       'year = ' + str(year) + ' and matchup = "' + match_up + '" and count = "' + count + '"')) == 0:
            for pitch_type, total in pitch_type_dict.items():
                fields += ', ' + pitch_type
                values += ', ' + str(round(total/total_pitches, 3))
            executor1.submit(db.write('insert into pitch_usage_' + player_type + ' (uid, playerid, year, matchup, '
                                      'count' + fields + ', p_uid) values (default, "' + player_id + '", ' + str(year)
                                      + ', "' + match_up + '", "' + count + '"' + values + ', ' + str(p_uid) + ');'))
        else:
            sets = ''
            for pitch_type, total in pitch_type_dict.items():
                sets += pitch_type + ' = ' + str(round(total/total_pitches, 3)) + ', '
            if len(sets) > 0:
                executor1.submit(db.write('update pitch_usage_' + player_type + ' set ' + sets[:-2] + ' where playerid'
                                          ' = "' + player_id + '" and year = ' + str(year) + ' and matchup = "'
                                          + match_up + '" and count = "' + count + '";'))
    db.close()


def write_outcomes(player_id, p_uid, year, matchup, count, outcomes_by_pitch_type, player_type):
    db = DatabaseConnection(sandbox_mode=True)
    fields = ''
    total_pitches = {}
    for pitch_type, outcomes in outcomes_by_pitch_type.items():
        fields += ', ' + pitch_type + ' float'
        total_pitches[pitch_type] = 0
        for _, total in outcomes.items():
            total_pitches[pitch_type] += total
    with ThreadPoolExecutor(os.cpu_count()) as executor4:
        for pitch_type, outcomes in outcomes_by_pitch_type.items():
            for outcome, total in outcomes.items():
                if len(db.read('select * from outcomes_' + player_type + ' where playerid = "' + player_id + '" and'
                               ' year = ' + str(year) + ' and matchup = "' + matchup + '" and count = "' + count
                               + '" and outcome = "' + outcome + '";')) == 0:
                    executor4.submit(db.write('insert into outcomes_' + player_type + ' (uid, playerid, year, matchup, '
                                              'count, outcome, ' + pitch_type + ', p_uid) values (default, "'
                                              + player_id + '", ' + str(year) + ', "' + matchup + '", "' + count
                                              + '", "' + outcome + '", ' + str(round(total/total_pitches[pitch_type],
                                                                                     3)) + ', ' + str(p_uid) + ');'))
                else:
                    executor4.submit(db.write('update outcomes_' + player_type + ' set ' + pitch_type + ' = '
                                              + str(round(total/total_pitches[pitch_type], 3)) + ' where playerid = "'
                                              + player_id + '" and year = ' + str(year) + ' and matchup = "' + matchup
                                              + '" and count = "' + count + '" and outcome = "' + outcome + '" and '
                                              'p_uid = ' + str(p_uid) + ';'))
    db.close()


def write_swing_rate(player_id, p_uid, year, matchup, count, pitch_usage, swing_rates, player_type):
    db = DatabaseConnection(sandbox_mode=True)
    if len(db.read('select uid from swing_rate_' + player_type + ' where playerid = "' + player_id + '" and year = '
                   + str(year) + ' and matchup = "' + matchup + '" and count = "' + count + '"')) == 0:
        fields = ''
        values = ''
        for pitch_type, swings in swing_rates.items():
            fields += ', ' + pitch_type
            values += ', ' + str(round(swings/pitch_usage[pitch_type], 3))
        db.write('insert into swing_rate_' + player_type + ' (uid, playerid, year, matchup, count' + fields + ', p_uid)'
                 ' values (default, "' + player_id + '", ' + str(year) + ', "' + matchup + '", "' + count + '"'
                 + values + ', ' + str(p_uid) + ');')
    else:
        sets = ''
        for pitch_type, swings in swing_rates.items():
            sets + ' = ' + str(round(swings/pitch_usage[pitch_type], 3)) + ', '
        if len(sets) > 0:
            db.write('update swing_rate_' + player_type + ' set ' + sets[:-2] + ' where playerid = "' + player_id
                     + '", and year = ' + str(year) + ' and matchup = "' + matchup + '", and count = "' + count + '";')
    db.close()


def write_strike_percent(player_id, p_uid, year, matchup, count, pitch_usage, strike_percents, player_type):
    db = DatabaseConnection(sandbox_mode=True)
    if len(db.read('select uid from strike_percent_' + player_type + ' where playerid = "' + player_id + '" and year = '
                   + str(year) + ' and matchup = "' + matchup + '" and count = "' + count + '"')) == 0:
        fields = ''
        values = ''
        for pitch_type, strikes in strike_percents.items():
            fields += ', ' + pitch_type
            values += ', ' + str(round(strikes/pitch_usage[pitch_type], 3))
        db.write('insert into strike_percent_' + player_type + ' (uid, playerid, year, matchup, count' + fields
                 + ', p_uid) values (default, "' + player_id + '", ' + str(year) + ', "' + matchup + '", "' + count
                 + '"' + values + ', ' + str(p_uid) + ');')
    else:
        sets = ''
        for pitch_type, strikes in strike_percents.items():
            sets + ' = ' + str(round(strikes/pitch_usage[pitch_type], 3)) + ', '
        if len(sets) > 0:
            db.write('update strike_percent_' + player_type + ' set ' + sets[:-2] + ' where playerid = "' + player_id
                     + '", and year = ' + str(year) + ' and matchup = "' + matchup + '", and count = "' + count + '";')
    db.close()


def write_field_by_pitch_type(player_id, p_uid, year, matchup, count, field, player_type):
    db = DatabaseConnection(sandbox_mode=True)
    fields = ''
    total_pitches = {}
    for pitch_type, dictionary in field.items():
        fields += ', ' + pitch_type + ' float'
        total_pitches[pitch_type] = 0
        for _, total in dictionary.items():
            total_pitches[pitch_type] += total
    with ThreadPoolExecutor(os.cpu_count()) as executor4:
        for pitch_type, dictionary in field.items():
            for field_type, total in dictionary.items():
                if len(db.read('select * from field_' + player_type + ' where playerid = "' + player_id
                               + '" and year = ' + str(year) + ' and matchup = "' + matchup + '" and count = "' + count
                               + '" and field = "' + field_type + '";')) == 0:
                    executor4.submit(db.write('insert into field_' + player_type + ' (uid, playerid, year, matchup, '
                                              'count, field, ' + pitch_type + ', p_uid) values (default, "' + player_id
                                              + '", ' + str(year) + ', "' + matchup + '", "' + count + '", "'
                                              + field_type + '", ' + str(round(total/total_pitches[pitch_type], 3))
                                              + ', ' + str(p_uid) + ');'))
                else:
                    executor4.submit(db.write('update field_' + player_type + ' set ' + pitch_type + ' = '
                                              + str(round(total/total_pitches[pitch_type], 3)) + ' where playerid = "'
                                              + player_id + '" and year = ' + str(year) + ' and matchup = "' + matchup
                                              + '" and count = "' + count + '" and field = "' + field_type + '";'))
    db.close()


def write_trajectory_by_pitch_type(player_id, p_uid, year, matchup, count, trajectories, player_type):
    db = DatabaseConnection(sandbox_mode=True)
    fields = ''
    total_pitches = {}
    for pitch_type, trajectory in trajectories.items():
        fields += ', ' + pitch_type + ' float'
        total_pitches[pitch_type] = 0
        for _, total in trajectory.items():
            total_pitches[pitch_type] += total
    with ThreadPoolExecutor(os.cpu_count()) as executor4:
        for pitch_type, trajectory in trajectories.items():
            for trajectory_type, total in trajectory.items():
                if len(db.read('select * from trajectory_' + player_type + ' where playerid = "' + player_id
                               + '" and year = ' + str(year) + ' and matchup = "' + matchup + '" and count = "' + count
                               + '" and trajectory = "' + trajectory_type + '";')) == 0:
                    executor4.submit(db.write('insert into trajectory_' + player_type + ' (uid, playerid, year, '
                                              'matchup, count, trajectory,' + ' ' + pitch_type + ', p_uid) values '
                                              '(default, "' + player_id + '", ' + str(year) + ', "' + matchup + '", "'
                                              + count + '", "' + trajectory_type + '", '
                                              + str(round(total/total_pitches[pitch_type], 3))
                                              + ', ' + str(p_uid) + ');'))
                else:
                    executor4.submit(db.write('update trajectory_' + player_type + ' set ' + pitch_type + ' = '
                                              + str(round(total/total_pitches[pitch_type], 3)) + ' where playerid = "'
                                              + player_id + '" and year = ' + str(year) + ' and matchup = "' + matchup
                                              + '" and count = "' + count + '" and trajectory = "' + trajectory_type
                                              + '";'))
    db.close()


def write_direction_by_pitch_type(player_id, p_uid, year, matchup, count, direction, player_type):
    db = DatabaseConnection(sandbox_mode=True)
    fields = ''
    total_pitches = {}
    for pitch_type, dictionary in direction.items():
        fields += ', ' + pitch_type + ' float'
        total_pitches[pitch_type] = 0
        for _, total in dictionary.items():
            total_pitches[pitch_type] += total
    with ThreadPoolExecutor(os.cpu_count()) as executor4:
        for pitch_type, dictionary in direction.items():
            for direction_type, total in dictionary.items():
                if len(db.read('select * from direction_' + player_type + ' where playerid = "' + player_id
                               + '" and year = ' + str(year) + ' and matchup = "' + matchup + '" and count = "' + count
                               + '" and direction = "' + direction_type + '";')) == 0:
                    executor4.submit(db.write('insert into direction_' + player_type + ' (uid, playerid, year, matchup,'
                                              ' count, direction, ' + pitch_type + ', p_uid) values (default, "'
                                              + player_id + '", ' + str(year) + ', "' + matchup + '", "' + count
                                              + '", "' + direction_type + '", '
                                              + str(round(total/total_pitches[pitch_type], 3)) + ', '
                                              + str(p_uid) + ');'))
                else:
                    executor4.submit(db.write('update direction_' + player_type + ' set ' + pitch_type + ' = '
                                              + str(round(total/total_pitches[pitch_type], 3)) + ' where playerid = "'
                                              + player_id + '" and year = ' + str(year) + ' and matchup = "'
                                              + matchup + '" and count = "' + count + '" and direction = "'
                                              + direction_type + '";'))
    db.close()


def write_trajectory_by_outcome(player_id, p_uid, year, matchup, trajectory_by_outcome, player_type):
    db = DatabaseConnection(sandbox_mode=True)
    outcome_totals = {}
    for outcome, dictionary in trajectory_by_outcome.items():
        for trajectory, total in dictionary.items():
            if outcome in outcome_totals:
                outcome_totals[outcome] += total
            else:
                outcome_totals[outcome] = total
        with ThreadPoolExecutor(os.cpu_count()) as executor:
            for trajectory, total in dictionary.items():
                if len(db.read('select * from trajectory_by_outcome_' + player_type + ' where playerid = "' + player_id
                               + '" and year = ' + str(year) + ' and matchup = "' + matchup + '" and outcome = "'
                               + outcome + '";')) == 0:
                    executor.submit(db.write('insert into trajectory_by_outcome_' + player_type + ' (uid, playerid, '
                                             'year, matchup, outcome, ' + trajectory + ', p_uid) values (default, "'
                                             + player_id + '", ' + str(year) + ', "' + matchup + '", "' + outcome + '",'
                                             + str(round(total/outcome_totals[outcome], 3)) + ', ' + str(p_uid) + ');'))
                else:
                    executor.submit(db.write('update trajectory_by_outcome_' + player_type + ' set ' + trajectory
                                             + ' = ' + str(round(total/outcome_totals[outcome], 3)) + ' where playerid '
                                             '= "' + player_id + '" and year = ' + str(year) + ' and matchup = "'
                                             + matchup + '" and outcome = "' + outcome + '";'))
    db.close()


def write_field_by_outcome(player_id, p_uid, year, matchup, field_by_outcome, player_type):
    db = DatabaseConnection(sandbox_mode=True)
    outcome_totals = {}
    for outcome, dictionary in field_by_outcome.items():
        for field, total in dictionary.items():
            if outcome in outcome_totals:
                outcome_totals[outcome] += total
            else:
                outcome_totals[outcome] = total
        with ThreadPoolExecutor(os.cpu_count()) as executor:
            for field, total in dictionary.items():
                if len(db.read('select * from field_by_outcome_' + player_type + ' where playerid = "' + player_id
                               + '" and year = ' + str(year) + ' and matchup = "' + matchup + '" and outcome = "'
                               + outcome + '";')) == 0:
                    executor.submit(db.write('insert into field_by_outcome_' + player_type + ' (uid, playerid, year, '
                                             'matchup, outcome, ' + field + ', p_uid) values (default, "' + player_id
                                             + '", ' + str(year) + ', "' + matchup + '", "' + outcome + '", '
                                             + str(round(total/outcome_totals[outcome], 3)) + ', ' + str(p_uid) + ');'))
                else:
                    executor.submit(db.write('update field_by_outcome_' + player_type + ' set ' + field + ' = '
                                             + str(round(total/outcome_totals[outcome], 3)) + ' where playerid = "'
                                             + player_id + '" and year = ' + str(year) + ' and matchup = "' + matchup
                                             + '" and outcome = "' + outcome + '";'))
    db.close()


def write_direction_by_outcome(player_id, p_uid, year, match_up, direction_by_outcome, player_type):
    db = DatabaseConnection(sandbox_mode=True)
    outcome_totals = {}
    for outcome, dictionary in direction_by_outcome.items():
        for direction, total in dictionary.items():
            if outcome in outcome_totals:
                outcome_totals[outcome] += total
            else:
                outcome_totals[outcome] = total
        with ThreadPoolExecutor(os.cpu_count()) as executor:
            for direction, total in dictionary.items():
                if len(db.read('select * from direction_by_outcome_' + player_type + ' where playerid = "' + player_id
                               + '" and year = ' + str(year) + ' and matchup = "' + match_up + '" and outcome = "'
                               + outcome + '";')) == 0:
                    executor.submit(db.write('insert into direction_by_outcome_' + player_type + ' (uid, playerid, '
                                             'year, matchup, outcome, ' + direction + ', p_uid) values (default, "'
                                             + player_id + '", ' + str(year) + ', "' + match_up + '", "' + outcome + '",'
                                             + str(round(total/outcome_totals[outcome], 3)) + ', ' + str(p_uid) + ');'))
                else:
                    executor.submit(db.write('update direction_by_outcome_' + player_type + ' set ' + direction + ' = '
                                             + str(round(total/outcome_totals[outcome], 3)) + ' where playerid = "'
                                             + player_id + '" and year = ' + str(year) + ' and matchup = "' + match_up
                                             + '" and outcome = "' + outcome + '";'))
    db.close()


def sort_by_pitch_type(pitches, match_up, balls, strikes):
    pitches_by_type = {}
    for pitch in pitches:
        if pitch[5][:2] == match_up:
            if int(pitch[6][0]) == balls and int(pitch[6][-1]) == strikes:
                if pitch[7] in pitches_by_type:
                    pitches_by_type[pitch[7]].append(pitch[8:])
                else:
                    pitches_by_type[pitch[7]] = [pitch[8:]]
    return pitches_by_type


def sort_further_by_outcome(pitches):
    outcomes = {}
    for pitch in pitches:
        if pitch[2] not in outcomes:
            outcomes[pitch[2]] = 1
        else:
            outcomes[pitch[2]] += 1
    return outcomes


def calculate_strike_percent(pitches):
    strikes = 0
    for pitch in pitches:
        if pitch[1] == 'strike':
            strikes += 1
    return strikes / len(pitches)


def calculate_swing_rate(pitches):
    swings = 0
    for pitch in pitches:
        if pitch[0] == 'swing':
            swings += 1
    return swings / len(pitches)


def sort_further_by_trajectory(pitches):
    trajectories = {}
    for pitch in pitches:
        if pitch[3] in trajectories:
            trajectories[pitch[3]] += 1
        else:
            trajectories[pitch[3]] = 1
    return trajectories


def sort_further_by_field(pitches):
    fields = {}
    for pitch in pitches:
        if pitch[4] in fields:
            fields[pitch[4]] += 1
        else:
            fields[pitch[4]] = 1
    return fields


def sort_further_by_direction(pitches):
    direction = {}
    for pitch in pitches:
        if pitch[5] in direction:
            direction[pitch[5]] += 1
        else:
            direction[pitch[5]] = 1
    return direction


def sort_pitches_by_outcome(pitches):
    outcomes = {}
    for pitch in pitches:
        if pitch[10] in outcomes:
            outcomes[pitch[10]].append(pitch[11:])
        else:
            outcomes[pitch[10]] = [pitch[11:]]
    return outcomes


def calculate_trajectory_by_outome(pitches):
    trajectories = {}
    for pitch in pitches:
        if pitch[0] in trajectories:
            trajectories[pitch[0]] += 1
        else:
            trajectories[pitch[0]] = 1
    return trajectories


def calculate_field_by_outcome(pitches):
    fields = {}
    for pitch in pitches:
        if pitch[1] in fields:
            fields[pitch[1]] += 1
        else:
            fields[pitch[1]] = 1
    return fields


def calculate_direction_by_outcome(pitches):
    directions = {}
    for pitch in pitches:
        if pitch[2] in directions:
            directions[pitch[2]] += 1
        else:
            directions[pitch[2]] = 1
    return directions


def change_multi_team_players_uids(year, month, day, player_type):
    start_time = time.time()
    logger.log('\tChanging ' + player_type + ' unique identifiers for players who played on more than one team')
    pitch_fx_db = PitchFXDatabaseConnection(sandbox_mode=True)
    if month is None and day is None:
        uids_players = pitch_fx_db.read('select  playerId from ' + player_type[:-3] + 'er_pitches where year = '
                                        + str(year) + ' group by p' + player_type[0] + '_uniqueidentifier;')
    else:
        uids_players = pitch_fx_db.read('select  playerId from ' + player_type[:-3] + 'er_pitches where year = '
                                        + str(year) + ' and month = ' + str(month) + ' and day = ' + str(day)
                                        + ' group by p' + player_type[0] + '_uniqueidentifier;')
    pitch_fx_db.close()
    for uid_player in uids_players:
        baseball_data_db = DatabaseConnection(sandbox_mode=True)
        try:
            pt_uid_tot = baseball_data_db.read('select pt_uniqueidentifier from player_teams where playerId = "'
                                               + uid_player[0] + '" and teamId = "TOT";')[0][0]
        except IndexError:
            baseball_data_db.close()
            continue
        try:
            p_uid_for_tot_stats = \
                baseball_data_db.read('select p' + player_type[0] + '_uniqueidentifier from player_' + player_type
                                      + ' where year = ' + str(year) + ' and pt_uniqueidentifier = ' + str(pt_uid_tot)
                                      + ';')[0][0]
            baseball_data_db.close()
            pitch_fx_db = PitchFXDatabaseConnection(sandbox_mode=True)
            pitch_fx_db.write('update ' + player_type[:-3] + 'er_pitches set p' + player_type[0] + '_uniqueidentifier'
                              ' = ' + str(p_uid_for_tot_stats) + ' where playerId = "' + uid_player[0] + '";')
            pitch_fx_db.close()
        except IndexError:
            baseball_data_db.close()
    logger.log('\t\tTime = ' + time_converter(time.time() - start_time))


# aggregate_pitch_fx(2017)
# aggregate(2017, None, None, 'breslcr01', 'pitching')
