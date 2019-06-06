import os
from utilities.database.wrappers.baseball_data_connection import DatabaseConnection
from utilities.database.wrappers.pitch_fx_connection import PitchFXDatabaseConnection
from utilities.logger import Logger
import time
import datetime
from concurrent.futures import ThreadPoolExecutor
from utilities.time_converter import time_converter
from utilities.properties import sandbox_mode, log_prefix, import_driver_logger as driver_logger

logger = Logger(os.path.join(log_prefix, "import_data", "aggregate_pitch_fx_data.log"))


def aggregate_pitch_fx_data(year, month=None, day=None):
    print('Aggregating pitch fx data')
    driver_logger.log("\tAggregating pitch fx data")
    start_time = time.time()
    logger.log("Aggregating pitch fx data for " + str(year) + ' || Timestamp: ' + datetime.datetime.today().
               strftime('%Y-%m-%d %H:%M:%S'))
    aggregate_and_write(year, month, day, 'pitching')
    aggregate_and_write(year, month, day, 'batting')
    total_time = time_converter(time.time() - start_time)
    logger.log("Done aggregating " + str(year) + " pitch fx data: Time = " + total_time + '\n\n')
    driver_logger.log("\t\tTime = " + total_time)


def aggregate_and_write(year, month, day, player_type):
    start_time = time.time()
    driver_logger.log("\t\tAggregating " + player_type + " data")
    logger.log("\tAggregating " + player_type + " data and writing to database")
    db = PitchFXDatabaseConnection(sandbox_mode)
    extended_query = ''
    if month is not None and day is not None:
        extended_query += ' and month = ' + str(month) + ' and day = ' + str(day)
    players = set(db.read('select playerid from ' + player_type[:-3] + 'er_pitches where year = ' + str(year)
                          + extended_query + ';'))
    logger.log(len(players))
    logger.log(players)
    db.close()
    for player_id in players:
        aggregate(year, player_id[0], player_type)
        # break
    total_time = time_converter(time.time()-start_time)
    logger.log("\tDone aggregating and writing " + player_type + " data: Time = " + total_time)
    driver_logger.log("\t\t\tTime = " + total_time)


def aggregate(year, player_id, player_type):
    logger.log('\t' + player_id)
    start_time = time.time()
    table = player_type[:-3] + 'er_pitches'
    matchups = ['vr', 'vl']
    opponent = 'hb' if player_type == 'pitching' else 'hp'
    pitch_usage = {}
    pitch_type_outcomes = {}
    pitch_type_swing_rate = {}
    strike_percents = {}
    trajectories = {}
    fields = {}
    directions = {}
    trajectories_by_outcome = {}
    fields_by_outcome = {}
    directions_by_outcome = {}
    db = DatabaseConnection(sandbox_mode)
    temp_p_uid = []
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
    pitch_fx_db = PitchFXDatabaseConnection(sandbox_mode)
    for matchup in matchups:
        aggregate_hbp(player_id, year, matchup, p_uid, player_type)
        pitch_usage[matchup] = {}
        pitch_type_outcomes[matchup] = {}
        pitch_type_swing_rate[matchup] = {}
        strike_percents[matchup] = {}
        trajectories[matchup] = {}
        fields[matchup] = {}
        directions[matchup] = {}
        trajectories_by_outcome[matchup] = {}
        fields_by_outcome[matchup] = {}
        directions_by_outcome[matchup] = {}
        for ball in range(4):
            for strike in range(3):
                count = str(ball) + '-' + str(strike)
                pitch_usage[matchup][count] = {}
                pitch_type_outcomes[matchup][count] = {}
                pitch_type_swing_rate[matchup][count] = {}
                strike_percents[matchup][count] = {}
                trajectories[matchup][count] = {}
                fields[matchup][count] = {}
                directions[matchup][count] = {}
                bulk_query = 'from ' + table + ' where playerid = "' + player_id + '" and year = ' + str(year)\
                             + ' and matchup = "' + matchup + opponent + '" and count = "' + count + '"'
                for pitch_type in set(pitch_fx_db.read('select pitch_type ' + bulk_query + 'and swing_take = '
                                                                                           '"swing";')):
                    temp_bulk_query = bulk_query + ' and pitch_type = "' + pitch_type[0] + '"'
                    pitch_usage[matchup][count][pitch_type[0]] = \
                        int(pitch_fx_db.read('select count(*) ' + temp_bulk_query + ';')[0][0])
                    pitch_type_outcomes[matchup][count][pitch_type[0]] = {}
                    pitch_type_swing_rate[matchup][count][pitch_type[0]] = \
                        pitch_fx_db.read('select count(*) ' + temp_bulk_query + ' and swing_take = "swing";')[0][0]
                    strike_percents[matchup][count][pitch_type[0]] = \
                        pitch_fx_db.read('select count(*) ' + temp_bulk_query + ' and ball_strike = "strike";')[0][0]
                    trajectories[matchup][count][pitch_type[0]] = {}
                    fields[matchup][count][pitch_type[0]] = {}
                    directions[matchup][count][pitch_type[0]] = {}
                    for outcome in pitch_fx_db.read('select outcome ' + temp_bulk_query + ' and swing_take = "swing";'):
                        if outcome[0] in pitch_type_outcomes[matchup][count][pitch_type[0]]:
                            pitch_type_outcomes[matchup][count][pitch_type[0]][outcome[0]] += 1
                        else:
                            pitch_type_outcomes[matchup][count][pitch_type[0]][outcome[0]] = 1
                    for trajectory in pitch_fx_db.read('select trajectory ' + temp_bulk_query + ';'):
                        if trajectory[0] in trajectories[matchup][count][pitch_type[0]]:
                            trajectories[matchup][count][pitch_type[0]][trajectory[0]] += 1
                        else:
                            trajectories[matchup][count][pitch_type[0]][trajectory[0]] = 1
                    for field in pitch_fx_db.read('select field ' + temp_bulk_query + ';'):
                        if field[0] in fields[matchup][count][pitch_type[0]]:
                            fields[matchup][count][pitch_type[0]][field[0]] += 1
                        else:
                            fields[matchup][count][pitch_type[0]][field[0]] = 1
                    for direction in pitch_fx_db.read('select direction ' + temp_bulk_query + ';'):
                        if direction[0] in directions[matchup][count][pitch_type[0]]:
                            directions[matchup][count][pitch_type[0]][direction[0]] += 1
                        else:
                            directions[matchup][count][pitch_type[0]][direction[0]] = 1
                write_pitch_usage(player_id, p_uid, year, matchup, count, pitch_usage[matchup][count], player_type)
                write_outcomes(player_id, p_uid, year, matchup, count, pitch_type_outcomes[matchup][count], player_type)
                write_swing_rate(player_id, p_uid, year, matchup, count, pitch_usage[matchup][count],
                                 pitch_type_swing_rate[matchup][count], player_type)
                write_strike_percent(player_id, p_uid, year, matchup, count, pitch_usage[matchup][count],
                                     strike_percents[matchup][count], player_type)
                write_trajectory_by_pitch_type(player_id, p_uid, year, matchup, count, trajectories[matchup][count],
                                               player_type)
                write_field_by_pitch_type(player_id, p_uid, year, matchup, count, fields[matchup][count], player_type)
                write_direction_by_pitch_type(player_id, p_uid, year, matchup, count, directions[matchup][count],
                                              player_type)
        for outcome in set(pitch_fx_db.read('select outcome from ' + table + ' where playerid = "' + player_id + '" and'
                                            ' year = ' + str(year) + ' and matchup = "' + matchup + opponent + '";')):
            trajectories_by_outcome[matchup][outcome[0]] = {}
            for trajectory in pitch_fx_db.read('select trajectory from ' + table + ' where playerid = "' + player_id
                                               + '" and year = ' + str(year) + ' and matchup = "' + matchup + opponent
                                               + '" and outcome = "' + outcome[0] + '";'):
                if trajectory[0] in trajectories_by_outcome[matchup][outcome[0]]:
                    trajectories_by_outcome[matchup][outcome[0]][trajectory[0]] += 1
                else:
                    trajectories_by_outcome[matchup][outcome[0]][trajectory[0]] = 1
            fields_by_outcome[matchup][outcome[0]] = {}
            for field in pitch_fx_db.read('select field from ' + table + ' where playerid = "' + player_id + '" and '
                                          'year = ' + str(year) + ' and matchup = "' + matchup + opponent + '" and '
                                          'outcome = "' + outcome[0] + '";'):
                if field[0] in fields_by_outcome[matchup][outcome[0]]:
                    fields_by_outcome[matchup][outcome[0]][field[0]] += 1
                else:
                    fields_by_outcome[matchup][outcome[0]][field[0]] = 1
            directions_by_outcome[matchup][outcome[0]] = {}
            for direction in pitch_fx_db.read('select direction from ' + table + ' where playerid = "' + player_id
                                              + '" and year = ' + str(year) + ' and matchup = "' + matchup + opponent
                                              + '" and outcome = "' + outcome[0] + '";'):
                if direction[0] in directions_by_outcome[matchup][outcome[0]]:
                    directions_by_outcome[matchup][outcome[0]][direction[0]] += 1
                else:
                    directions_by_outcome[matchup][outcome[0]][direction[0]] = 1
        write_trajectory_by_outcome(player_id, p_uid, year, matchup, trajectories_by_outcome[matchup], player_type)
        write_field_by_outcome(player_id, p_uid, year, matchup, fields_by_outcome[matchup], player_type)
        write_direction_by_outcome(player_id, p_uid, year, matchup, directions_by_outcome[matchup], player_type)
    pitch_fx_db.close()
    db.close()
    logger.log('\t\tTime = ' + time_converter(time.time()-start_time))


def aggregate_hbp(player_id, year, matchup, p_uid, player_type):
    if player_type != 'pitching':
        return
    pitches = {}
    db = PitchFXDatabaseConnection(sandbox_mode)
    hbps = db.read('select pitch_type, count(*) from pitcher_pitches where year = ' + str(year) + ' and playerid'
                   ' = "' + player_id + '" and matchup = "' + matchup + 'hb" and outcome = "hbp" group by '
                   'pitch_type;')
    for pitch_type in db.read('select pitch_type, count(*) from pitcher_pitches where year = ' + str(year) + ' and '
                              'playerid = "' + player_id + '" and matchup="' + matchup + 'hb" group by pitch_type;'):
        pitches[pitch_type[0]] = pitch_type[1]
    db.close()
    new_db = DatabaseConnection(sandbox_mode)
    if len(new_db.read('select hbp_id from hbp where playerid = "' + player_id + '" and year = ' + str(year)
                       + ' and matchup = "' + matchup + '";')) == 0:
        fields = ''
        values = ''
        for hbp_by_pitch_type in hbps:
            fields += ', ' + hbp_by_pitch_type[0]
            values += ', ' + str(round(hbp_by_pitch_type[1]/pitches[hbp_by_pitch_type[0]], 3))
        new_db.write('insert into hbp (hbp_id, playerid, year, matchup' + fields + ', p_uid) values (default, "'
                     + player_id + '", ' + str(year) + ', "' + matchup + '"' + values + ', ' + str(p_uid) + ');')
    else:
        sets = ''
        for hbp_by_pitch_type in hbps:
            sets += hbp_by_pitch_type[0] + '=' + str(round(hbp_by_pitch_type[1]/pitches[hbp_by_pitch_type[0]], 3)) + ','
        if len(sets) > 0:
            new_db.write('update hbp set ' + sets[:-1] + 'where playerid = "' + player_id + '" and year = ' + str(year)
                         + ' and matchup = "' + matchup + '";')
    new_db.close()


def write_pitch_usage(player_id, p_uid, year, matchup, count, pitch_type_dict, player_type):
    if player_type != 'pitching':
        return
    db = DatabaseConnection(sandbox_mode)
    total_pitches = 0
    fields = ''
    values = ''
    for pitch_type, total in pitch_type_dict.items():
        total_pitches += total
    with ThreadPoolExecutor(os.cpu_count()) as executor1:
        if len(db.read('select uid from pitch_usage where playerid = "' + player_id + '" and year = ' + str(year)
                       + ' and matchup = "' + matchup + '" and count = "' + count + '"')) == 0:
            for pitch_type, total in pitch_type_dict.items():
                fields += ', ' + pitch_type
                values += ', ' + str(round(total/total_pitches, 3))
            executor1.submit(db.write('insert into pitch_usage (uid, playerid, year, matchup, count' + fields
                                      + ', p_uid) values (default, "' + player_id + '", ' + str(year) + ', "' + matchup
                                      + '", "' + count + '"' + values + ', ' + str(p_uid) + ');'))
        else:
            sets = ''
            for pitch_type, total in pitch_type_dict.items():
                sets += pitch_type + ' = ' + str(round(total/total_pitches, 3)) + ', '
            if len(sets) > 0:
                executor1.submit(db.write('update pitch_usage set ' + sets[:-2] + ' where playerid = "' + player_id
                                          + '" and year = ' + str(year) + ' and matchup = "' + matchup
                                          + '" and count = "' + count + '";'))
    db.close()


def write_outcomes(player_id, p_uid, year, matchup, count, outcomes_by_pitch_type, player_type):
    db = DatabaseConnection(sandbox_mode)
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
    db = DatabaseConnection(sandbox_mode)
    if len(db.read('select uid from swing_rate_' + player_type + ' where playerid = "' + player_id + '" and year = '
                   + str(year) + ' and matchup = "' + matchup + '" and count = "' + count + '"')) == 0:
        fields = ''
        values = ''
        for pitch_type, swings in swing_rates.items():
            fields += ', ' + pitch_type
            values += ', ' + str(round(swings/pitch_usage[pitch_type], 3))
        db.write('insert into swing_rate_' + player_type + ' (uid, playerid, year, matchup, count' + fields + ', p_uid)'
                 ' values (default, "' + player_id + '", ' + str(year) + ', "' + matchup + '", "' + count + '"'
                 + values + ', p_uid = ' + str(p_uid) + ');')
    else:
        sets = ''
        for pitch_type, swings in swing_rates.items():
            sets + ' = ' + str(round(swings/pitch_usage[pitch_type], 3)) + ', '
        if len(sets) > 0:
            db.write('update swing_rate_' + player_type + ' set ' + sets[:-2] + ' where playerid = "' + player_id
                     + '", and year = ' + str(year) + ' and matchup = "' + matchup + '", and count = "' + count + '";')
    db.close()


def write_strike_percent(player_id, p_uid, year, matchup, count, pitch_usage, strike_percents, player_type):
    if player_type != 'pitching':
        return
    db = DatabaseConnection(sandbox_mode)
    if len(db.read('select uid from strike_percent_' + player_type + ' where playerid = "' + player_id + '" and year = '
                   + str(year) + ' and matchup = "' + matchup + '" and count = "' + count + '"')) == 0:
        fields = ''
        values = ''
        for pitch_type, strikes in strike_percents.items():
            fields += ', ' + pitch_type
            values += ', ' + str(round(strikes/pitch_usage[pitch_type], 3))
        db.write('insert into strike_percent_' + player_type + ' (uid, playerid, year, matchup, count' + fields
                 + ', p_uid) values (default, "' + player_id + '", ' + str(year) + ', "' + matchup + '", "' + count
                 + '"' + values + ', p_uid = ' + str(p_uid) + ');')
    else:
        sets = ''
        for pitch_type, strikes in strike_percents.items():
            sets + ' = ' + str(round(strikes/pitch_usage[pitch_type], 3)) + ', '
        if len(sets) > 0:
            db.write('update strike_percent_' + player_type + ' set ' + sets[:-2] + ' where playerid = "' + player_id
                     + '", and year = ' + str(year) + ' and matchup = "' + matchup + '", and count = "' + count + '";')
    db.close()


def write_trajectory_by_pitch_type(player_id, p_uid, year, matchup, count, trajectories, player_type):
    db = DatabaseConnection(sandbox_mode)
    fields = ''
    total_pitches = {}
    for pitch_type, trajectory in trajectories.items():
        fields += ', ' + pitch_type + ' float'
        total_pitches[pitch_type] = 0
        for _, total in trajectory.items():
            total_pitches[pitch_type] += total
    with ThreadPoolExecutor(os.cpu_count()) as executor4:
        for pitch_type, trajectory in trajectories.items():
            for trajectory, total in trajectory.items():
                if len(db.read('select * from trajectory_' + player_type + ' where playerid = "' + player_id
                               + '" and year = ' + str(year) + ' and matchup = "' + matchup + '" and count = "' + count
                               + '" and trajectory = "' + trajectory + '";')) == 0:
                    executor4.submit(db.write('insert into trajectory_' + player_type + ' (uid, playerid, year, '
                                              'matchup, count, trajectory,' + ' ' + pitch_type + ', p_uid) values '
                                              '(default, "' + player_id + '", ' + str(year) + ', "' + matchup + '", "'
                                              + count + '", "' + trajectory + '", '
                                              + str(round(total/total_pitches[pitch_type], 3))
                                              + ', ' + str(p_uid) + ');'))
                else:
                    executor4.submit(db.write('update trajectory_' + player_type + ' set ' + pitch_type + ' = '
                                              + str(round(total/total_pitches[pitch_type], 3)) + ' where playerid = "'
                                              + player_id + '" and year = ' + str(year) + ' and matchup = "' + matchup
                                              + '" and count = "' + count + '" and trajectory = "' + trajectory + '";'))
    db.close()


def write_field_by_pitch_type(player_id, p_uid, year, matchup, count, field, player_type):
    db = DatabaseConnection(sandbox_mode)
    fields = ''
    total_pitches = {}
    for pitch_type, dictionary in field.items():
        fields += ', ' + pitch_type + ' float'
        total_pitches[pitch_type] = 0
        for _, total in dictionary.items():
            total_pitches[pitch_type] += total
    with ThreadPoolExecutor(os.cpu_count()) as executor4:
        for pitch_type, dictionary in field.items():
            for field, total in dictionary.items():
                if len(db.read('select * from field_' + player_type + ' where playerid = "' + player_id
                               + '" and year = ' + str(year) + ' and matchup = "' + matchup + '" and count = "' + count
                               + '" and field = "' + field + '";')) == 0:
                    executor4.submit(db.write('insert into field_' + player_type + ' (uid, playerid, year, matchup, '
                                              'count, field, ' + pitch_type + ', p_uid) values (default, "' + player_id
                                              + '", ' + str(year) + ', "' + matchup + '", "' + count + '", "' + field
                                              + '", ' + str(round(total/total_pitches[pitch_type], 3)) + ', '
                                              + str(p_uid) + ');'))
                else:
                    executor4.submit(db.write('update field_' + player_type + ' set ' + pitch_type + ' = '
                                              + str(round(total/total_pitches[pitch_type], 3)) + ' where playerid = "'
                                              + player_id + '" and year = ' + str(year) + ' and matchup = "' + matchup
                                              + '" and count = "' + count + '" and field = "' + field + '";'))
    db.close()


def write_direction_by_pitch_type(player_id, p_uid, year, matchup, count, direction, player_type):
    db = DatabaseConnection(sandbox_mode)
    fields = ''
    total_pitches = {}
    for pitch_type, dictionary in direction.items():
        fields += ', ' + pitch_type + ' float'
        total_pitches[pitch_type] = 0
        for _, total in dictionary.items():
            total_pitches[pitch_type] += total
    with ThreadPoolExecutor(os.cpu_count()) as executor4:
        for pitch_type, dictionary in direction.items():
            for direction, total in dictionary.items():
                if len(db.read('select * from direction_' + player_type + ' where playerid = "' + player_id
                               + '" and year = ' + str(year) + ' and matchup = "' + matchup + '" and count = "' + count
                               + '" and direction = "' + direction + '";')) == 0:
                    executor4.submit(db.write('insert into direction_' + player_type + ' (uid, playerid, year, matchup,'
                                              ' count, direction, ' + pitch_type + ', p_uid) values (default, "'
                                              + player_id + '", ' + str(year) + ', "' + matchup + '", "' + count
                                              + '", "' + direction + '", ' + str(round(total/total_pitches[pitch_type],
                                                                                       3)) + ', ' + str(p_uid) + ');'))
                else:
                    executor4.submit(db.write('update direction_' + player_type + ' set ' + pitch_type + ' = '
                                              + str(round(total/total_pitches[pitch_type], 3)) + ' where playerid = "'
                                              + player_id + '" and year = ' + str(year) + ' and matchup = "' + matchup
                                              + '" and count = "' + count + '" and direction = "' + direction + '";'))
    db.close()


def write_trajectory_by_outcome(player_id, p_uid, year, matchup, trajectory_by_outcome, player_type):
    db = DatabaseConnection(sandbox_mode)
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
    db = DatabaseConnection(sandbox_mode)
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


def write_direction_by_outcome(player_id, p_uid, year, matchup, direction_by_outcome, player_type):
    db = DatabaseConnection(sandbox_mode)
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
                               + '" and year = ' + str(year) + ' and matchup = "' + matchup + '" and outcome = "'
                               + outcome + '";')) == 0:
                    executor.submit(db.write('insert into direction_by_outcome_' + player_type + ' (uid, playerid, '
                                             'year, matchup, outcome, ' + direction + ', p_uid) values (default, "'
                                             + player_id + '", ' + str(year) + ', "' + matchup + '", "' + outcome + '",'
                                             + str(round(total/outcome_totals[outcome], 3)) + ', ' + str(p_uid) + ');'))
                else:
                    executor.submit(db.write('update direction_by_outcome_' + player_type + ' set ' + direction + ' = '
                                             + str(round(total/outcome_totals[outcome], 3)) + ' where playerid = "'
                                             + player_id + '" and year = ' + str(year) + ' and matchup = "' + matchup
                                             + '" and outcome = "' + outcome + '";'))
    db.close()
