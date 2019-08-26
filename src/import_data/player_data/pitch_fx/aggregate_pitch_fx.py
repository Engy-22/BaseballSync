import os
import json
import time
import datetime
import statistics as stat
from ast import literal_eval
from concurrent.futures import ThreadPoolExecutor
from utilities.database.wrappers.baseball_data_connection import DatabaseConnection
from utilities.database.wrappers.pitch_fx_connection import PitchFXDatabaseConnection
from utilities.logger import Logger
from utilities.time_converter import time_converter
from utilities.properties import log_prefix, import_driver_logger as driver_logger

logger = Logger(os.path.join(log_prefix, "import_data", "aggregate_pitch_fx.log"))
all_pitchers_batting_data = {"temp_pitch_location_batting": {}, "pitch_location_batting": {},
                             "outcome_by_direction_batting": {}, "direction_batting": {}, "field_batting": {},
                             "outcome_by_field_batting": {}, "outcomes_batting": {}, "temp_swing_rate_batting": {},
                             "pitch_count_batting": {}, "pitch_usage_batting": {}, "trajectory_batting": {},
                             "outcome_by_trajectory_batting": {}, "swing_rate_batting": {}}
league_pitching_data = {"temp_pitch_location_pitching": {}, "pitch_location_pitching": {},
                        "outcome_by_direction_pitching": {}, "direction_pitching": {}, "field_pitching": {},
                        "outcome_by_field_pitching": {}, "outcomes_pitching": {}, "temp_swing_rate_pitching": {},
                        "pitch_count_pitching": {}, "pitch_usage_pitching": {}, "trajectory_pitching": {},
                        "outcome_by_trajectory_pitching": {}, "swing_rate_pitching": {}}


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
    if player_type == 'batting':
        round_up_and_write_all_pitchers_batting_stats(year)
    else:
        round_up_and_write_league_pitching_stats(year)
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
    pitch_locations = {}
    trajectories = {}
    fields = {}
    directions = {}
    p_uid = get_player_unique_stat_id(year, player_type, player_id)
    pitches = get_player_pitches(p_uid, year, month, day, player_type)
    for match_up in match_ups:
        aggregate_hbp(player_id, year, match_up, p_uid, player_type)
        pitch_usage[match_up] = {}
        pitch_type_outcomes[match_up] = {}
        swing_rates[match_up] = {}
        pitch_locations[match_up] = {}
        trajectories[match_up] = {}
        fields[match_up] = {}
        directions[match_up] = {}
        for ball in range(4):
            for strike in range(3):
                count = str(ball) + '-' + str(strike)
                pitch_usage[match_up][count] = {}
                pitch_type_outcomes[match_up][count] = {}
                swing_rates[match_up][count] = {}
                pitch_locations[match_up][count] = {}
                trajectories[match_up][count] = {}
                fields[match_up][count] = {}
                directions[match_up][count] = {}
                for pitch_type, pitch_type_list in sort_by_pitch_type(pitches, match_up, ball, strike).items():
                    if player_type == 'pitching':
                        accumulate_league_pitch_data(pitch_type, pitch_type_list, match_up, count)
                    if this_batter_is_a_pitcher(p_uid):
                        accumulate_all_pitchers_batting_dict(pitch_type, pitch_type_list, match_up, count)
                    pitch_usage[match_up][count][pitch_type] = len(pitch_type_list)
                    pitch_type_outcomes[match_up][count][pitch_type] = sort_further_by_outcome(pitch_type_list)
                    swing_rates[match_up][count][pitch_type] = calculate_swing_rate(pitch_type_list)
                    pitch_locations[match_up][count][pitch_type] = calculate_pitch_locations(pitch_type_list)
                    trajectories[match_up][count][pitch_type] = sort_further_by_trajectory(pitch_type_list)
                    fields[match_up][count][pitch_type] = sort_further_by_field(pitch_type_list)
                    directions[match_up][count][pitch_type] = sort_further_by_direction(pitch_type_list)
                write_pitch_count(player_id, p_uid, year, match_up, count, pitch_usage[match_up][count], player_type)
                write_pitch_usage(player_id, p_uid, year, match_up, count, pitch_usage[match_up][count], player_type)
                write_swing_rate(player_id, p_uid, year, match_up, count, swing_rates[match_up][count], player_type)
                write_pitch_locations(player_id, p_uid, year, match_up, count, pitch_locations[match_up][count],
                                      player_type)
                write_outcomes(player_id, p_uid, year, match_up, count, pitch_type_outcomes[match_up][count],
                               player_type)
                write_trajectory_by_pitch_type(player_id, p_uid, year, match_up, count, trajectories[match_up][count],
                                               player_type)
                write_field_by_pitch_type(player_id, p_uid, year, match_up, count, fields[match_up][count], player_type)
                write_direction_by_pitch_type(player_id, p_uid, year, match_up, count, directions[match_up][count],
                                              player_type)
        write_outcome_by_trajectory(player_id, p_uid, year, match_up, sort_outcomes_by_trajectory(pitches, match_up),
                                    player_type)
        write_outcome_by_field(player_id, p_uid, year, match_up, sort_outcomes_by_field(pitches, match_up), player_type)
        write_outcome_by_direction(player_id, p_uid, year, match_up, sort_outcomes_by_direction(pitches, match_up),
                                   player_type)
    write_overall_pitch_usage(player_id, p_uid, year, pitch_usage, player_type)
    write_overall_pitch_locations(year, p_uid, pitches, player_id, player_type)
    write_overall_swing_rate(year, p_uid, pitches, player_id, player_type)
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


def write_overall_pitch_usage(player_id, p_uid, year, pitch_type_dict, player_type):
    pitches_by_type = {}
    total_pitches = 0
    for match_up, count_data in pitch_type_dict.items():
        for count, pitches in count_data.items():
            for pitch_type, pitch_count in pitches.items():
                if pitch_type in pitches_by_type:
                    pitches_by_type[pitch_type] += pitch_count
                else:
                    pitches_by_type[pitch_type] = pitch_count
                total_pitches += pitch_count
    db = DatabaseConnection(sandbox_mode=True)
    fields = ''
    total_values = ''
    with ThreadPoolExecutor(os.cpu_count()) as executor1:
        if len(db.read('select uid from overall_pitch_usage_' + player_type + ' where playerid = "' + player_id
                       + '" and year = ' + str(year) + ';')) == 0:
            for pitch_type, pitch_count in pitches_by_type.items():
                fields += ', ' + pitch_type
                total_values += ', ' + str(round(pitch_count/total_pitches, 2))
            executor1.submit(
                db.write('insert into overall_pitch_usage_' + player_type + ' (uid, playerid, year' + fields
                         + ', p_uid) values (default, "' + player_id + '", ' + str(year) + total_values + ', '
                         + str(p_uid) + ');'))
        else:
            sets = ''
            for pitch_type, pitch_count in pitches_by_type.items():
                sets += pitch_type + ' = ' + str(round(pitch_count/total_pitches, 2)) + ', '
            if len(sets) > 0:
                executor1.submit(db.write('update overall_pitch_usage_' + player_type + ' set ' + sets[:-2]
                                          + ' where playerid = "' + player_id + '" and year = ' + str(year) + ';'))
    db.close()


def write_overall_pitch_locations(year, p_uid, pitches, player_id, player_type):
    x_coordinates = {}
    y_coordinates = {}
    for pitch in pitches:
        if pitch[7] in x_coordinates:
            x_coordinates[pitch[7]].append(pitch[14])
            y_coordinates[pitch[7]].append(pitch[15])
        else:
            x_coordinates[pitch[7]] = [pitch[14]]
            y_coordinates[pitch[7]] = [pitch[15]]
    db = DatabaseConnection(sandbox_mode=True)
    if len(db.read('select uid from overall_pitch_location_' + player_type + ' where playerid = "' + player_id
                   + '" and year = ' + str(year) + ';')) == 0:
        fields = ''
        values = ''
        for axis, coordinates in {'x': x_coordinates, 'y': y_coordinates}.items():
            for pitch_type, locations in coordinates.items():
                if len(locations) > 1:
                    fields += pitch_type + '_' + axis + '_mean, ' + pitch_type + '_' + axis + '_stdev, '
                    values += str(round(stat.mean(locations), 3)) + ', ' + str(round(stat.stdev(locations), 3)) + ', '
        db.write('insert into overall_pitch_location_' + player_type + ' (uid, playerid, year, ' + fields + 'p_uid) '
                 'values (default, "' + player_id + '", ' + str(year) + ', ' + values + str(p_uid) + ');')
    else:
        sets = ''
        for axis, coordinates in {'x': x_coordinates, 'y': y_coordinates}.items():
            for pitch_type, locations in coordinates.items():
                if len(locations) > 1:
                    sets += pitch_type + '_' + axis + '_mean = ' + str(round(stat.mean(locations), 3)) + ', ' + \
                            pitch_type + '_' + axis + '_stdev = ' + str(round(stat.stdev(locations), 3)) + ', '
        if len(sets) > 0:
            db.write('update overall_pitch_location_' + player_type + ' set ' + sets[:-2] + ' where playerid = "'
                     + player_id + '" and year = ' + str(year) + ';')
    db.close()


def write_overall_swing_rate(year, p_uid, pitches, player_id, player_type):
    pitch_counts = {'ball': {}, 'strike': {}}
    swings = {'ball': {}, 'strike': {}}
    swing_rates = {}
    x_coordinate = strike_zone_coordinate('x')
    y_coordinate = strike_zone_coordinate('y')
    for pitch in pitches:
        pitch_type = pitch[7]
        if pitch_in_zone(pitch[14], pitch[15], x_coordinate, y_coordinate):
            if pitch_type in pitch_counts['strike']:
                pitch_counts['strike'][pitch_type] += 1
                if pitch[8] == 'swing':
                    swings['strike'][pitch_type] += 1
            else:
                pitch_counts['strike'][pitch_type] = 1
                if pitch[8] == 'swing':
                    swings['strike'][pitch_type] = 1
                else:
                    swings['strike'][pitch_type] = 0
        else:
            if pitch_type in pitch_counts['ball']:
                pitch_counts['ball'][pitch_type] += 1
                if pitch[8] == 'swing':
                    swings['ball'][pitch_type] += 1
            else:
                pitch_counts['ball'][pitch_type] = 1
                if pitch[8] == 'swing':
                    swings['ball'][pitch_type] = 1
                else:
                    swings['ball'][pitch_type] = 0
    db = DatabaseConnection(sandbox_mode=True)
    for location in ['ball', 'strike']:
        for pitch_thrown, total in pitch_counts[location].items():
            swing_rates[pitch_thrown] = str(round(swings[location][pitch_thrown]/total, 3))
        if len(db.read('select uid from overall_swing_rate_' + player_type + ' where playerid = "' + player_id
                       + '" and year = ' + str(year) + ' and strike = ' + str(location == 'strike') + ';')) == 0:
            fields = ''
            values = ''
            for pitch_type, swing_rate in swing_rates.items():
                fields += pitch_type + ', '
                values += swing_rate + ', '
            db.write('insert into overall_swing_rate_' + player_type + ' (uid, playerid, year, strike, ' + fields
                     + 'p_uid) values (default, "' + player_id + '", ' + str(year) + ', ' + str(location == 'strike')
                     + ', ' + values + str(p_uid) + ');')
        else:
            sets = ''
            for pitch_type, swing_rate in swing_rates.items():
                sets += pitch_type + ' = ' + swing_rate + ', '
            if len(sets) > 0:
                db.write('update overall_swing_rate_' + player_type + ' set ' + sets[:-2] + ' where playerid = "'
                         + player_id + '" and year = ' + str(year) + ' and strike = ' + str(location == 'strike') + ';')
    db.close()


def write_pitch_count(player_id, p_uid, year, match_up, count, pitch_type_dict, player_type):
    db = DatabaseConnection(sandbox_mode=True)
    fields = ''
    total_values = ''
    with ThreadPoolExecutor(os.cpu_count()) as executor1:
        if len(db.read('select uid from pitch_count_' + player_type + ' where playerid = "' + player_id + '" and '
                       'year = ' + str(year) + ' and matchup = "' + match_up + '" and count = "' + count + '"')) == 0:
            for pitch_type, total in pitch_type_dict.items():
                fields += ', ' + pitch_type
                total_values += ', ' + str(total)
            executor1.submit(
                db.write('insert into pitch_count_' + player_type + ' (uid, playerid, year, matchup, count' + fields
                         + ', p_uid) values (default, "' + player_id + '", ' + str(year) + ', "' + match_up + '", "'
                         + count + '"' + total_values + ', ' + str(p_uid) + ');'))
        else:
            sets = ''
            for pitch_type, total in pitch_type_dict.items():
                sets += pitch_type + ' = ' + str(total) + ', '
            if len(sets) > 0:
                executor1.submit(db.write('update pitch_usage_' + player_type + ' set ' + sets[:-2] + ' where playerid'
                                          ' = "' + player_id + '" and year = ' + str(year) + ' and matchup = "'
                                          + match_up + '" and count = "' + count + '";'))
    db.close()


def write_pitch_usage(player_id, p_uid, year, match_up, count, pitch_type_dict, player_type):
    db = DatabaseConnection(sandbox_mode=True)
    total_pitches = 0
    fields = ''
    percent_values = ''
    total_values = ''
    for pitch_type, total in pitch_type_dict.items():
        total_pitches += total
    with ThreadPoolExecutor(os.cpu_count()) as executor1:
        if len(db.read('select uid from pitch_usage_' + player_type + ' where playerid = "' + player_id + '" and '
                       'year = ' + str(year) + ' and matchup = "' + match_up + '" and count = "' + count + '"')) == 0:
            for pitch_type, total in pitch_type_dict.items():
                fields += ', ' + pitch_type
                percent_values += ', ' + str(round(total/total_pitches, 3))
                total_values += ', ' + str(total)
            executor1.submit(db.write('insert into pitch_usage_' + player_type + ' (uid, playerid, year, matchup, '
                                      'count' + fields + ', p_uid) values (default, "' + player_id + '", ' + str(year)
                                      + ', "' + match_up + '", "' + count + '"' + percent_values + ', ' + str(p_uid) + ');'))
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


def write_swing_rate(player_id, p_uid, year, match_up, count, swing_rates, player_type):
    db = DatabaseConnection(sandbox_mode=True)
    for location, strike in {'in_zone': True, 'out_of_zone': False}.items():
        if len(db.read('select uid from swing_rate_' + player_type + ' where playerid = "' + player_id + '" and year = '
                       + str(year) + ' and matchup = "' + match_up + '" and count = "' + count + '" and strike = '
                       + str(strike) + ';')) == 0:
            fields = ''
            values = ''
            for pitch_type, swing_rate in swing_rates.items():
                try:
                    values += ', ' + str(round(swing_rate[location], 3))
                    fields += ', ' + pitch_type
                except TypeError:
                    continue
            db.write('insert into swing_rate_' + player_type + ' (uid, playerid, year, matchup, count, strike' + fields
                     + ', p_uid) values (default, "' + player_id + '", ' + str(year) + ', "' + match_up + '", "'
                     + count + '", ' + str(strike) + values + ', ' + str(p_uid) + ');')
        else:
            sets = ''
            for pitch_type, swing_rate in swing_rates.items():
                try:
                    sets += pitch_type + ' = ' + str(round(swing_rate[location], 3)) + ', '
                except TypeError:
                    continue
            if len(sets) > 0:
                db.write('update swing_rate_' + player_type + ' set ' + sets[:-2] + ' where playerid = "' + player_id
                         + '" and year = ' + str(year) + ' and matchup = "' + match_up + '" and count = "' + count
                         + '" and strike = ' + str(strike) + ';')
    db.close()


def write_pitch_locations(player_id, p_uid, year, match_up, count, pitch_locations, player_type):
    db = DatabaseConnection(sandbox_mode=True)
    if len(db.read('select uid from pitch_location_' + player_type + ' where playerid = "' + player_id + '" and year = '
                   + str(year) + ' and matchup = "' + match_up + '" and count = "' + count + '"')) == 0:
        fields = ''
        values = ''
        for pitch_type, pitch_location_data in pitch_locations.items():
            for pitch_stat, value in pitch_location_data.items():
                if value:
                    fields += ', ' + pitch_type + '_' + pitch_stat
                    values += ', ' + str(value)
        db.write('insert into pitch_location_' + player_type + ' (uid, playerid, year, matchup, count' + fields
                 + ', p_uid) values (default, "' + player_id + '", ' + str(year) + ', "' + match_up + '", "' + count
                 + '"' + values + ', ' + str(p_uid) + ');')
    else:
        sets = ''
        for pitch_type, pitch_location_data in pitch_locations.items():
            for pitch_stat, value in pitch_location_data.items():
                if value:
                    sets += pitch_type + '_' + pitch_stat + ' = ' + str(value) + ', '
        if len(sets) > 0:
            db.write('update pitch_location_' + player_type + ' set ' + sets[:-2] + ' where playerid = "' + player_id
                     + '" and year = ' + str(year) + ' and matchup = "' + match_up + '" and count = "' + count + '";')
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


def write_outcome_by_trajectory(player_id, p_uid, year, match_up, outcome_by_trajectory, player_type):
    db = DatabaseConnection(sandbox_mode=True)
    trajectory_totals = {}
    for trajectory, outcomes in outcome_by_trajectory.items():
        for outcome, total in outcomes.items():
            if trajectory in trajectory_totals:
                trajectory_totals[trajectory] += total
            else:
                trajectory_totals[trajectory] = total
        with ThreadPoolExecutor(os.cpu_count()) as executor:
            for outcome, total in outcomes.items():
                if len(db.read('select * from outcome_by_trajectory_' + player_type + ' where playerid = "' + player_id
                               + '" and year = ' + str(year) + ' and matchup = "' + match_up + '" and trajectory = "'
                               + trajectory + '";')) == 0:
                    executor.submit(db.write('insert into outcome_by_trajectory_' + player_type + ' (uid, playerid, '
                                             'year, matchup, trajectory, '
                                             + outcome.replace(' ', '_').replace(')', '').replace('(', '') + ', p_uid) '
                                             'values (default, "' + player_id + '", ' + str(year) + ', "' + match_up
                                             + '", "' + trajectory + '", '
                                             + str(round(total/trajectory_totals[trajectory], 3)) + ', ' + str(p_uid)
                                             + ');'))
                else:
                    executor.submit(db.write('update outcome_by_trajectory_' + player_type + ' set '
                                             + outcome.replace(' ', '_').replace(')', '').replace('(', '') + ' = '
                                             + str(round(total/trajectory_totals[trajectory], 3)) + ' where playerid '
                                             '= "' + player_id + '" and year = ' + str(year) + ' and matchup = "'
                                             + match_up + '" and trajectory = "' + trajectory + '";'))
    db.close()


def write_outcome_by_field(player_id, p_uid, year, match_up, outcome_by_field, player_type):
    db = DatabaseConnection(sandbox_mode=True)
    field_totals = {}
    for field, outcomes in outcome_by_field.items():
        for outcome, total in outcomes.items():
            if field in field_totals:
                field_totals[field] += total
            else:
                field_totals[field] = total
        with ThreadPoolExecutor(os.cpu_count()) as executor:
            for outcome, total in outcomes.items():
                if len(db.read('select * from outcome_by_field_' + player_type + ' where playerid = "' + player_id
                               + '" and year = ' + str(year) + ' and matchup = "' + match_up + '" and field = "'
                               + field + '";')) == 0:
                    executor.submit(db.write('insert into outcome_by_field_' + player_type + ' (uid, playerid, '
                                             'year, matchup, field, '
                                             + outcome.replace(' ', '_').replace(')', '').replace('(', '') + ', p_uid) '
                                             'values (default, "' + player_id + '", ' + str(year) + ', "' + match_up
                                             + '", "' + field + '", ' + str(round(total/field_totals[field], 3)) + ', '
                                             + str(p_uid) + ');'))
                else:
                    executor.submit(db.write('update outcome_by_field_' + player_type + ' set '
                                             + outcome.replace(' ', '_').replace(')', '').replace('(', '') + ' = '
                                             + str(round(total/field_totals[field], 3)) + ' where playerid '
                                             '= "' + player_id + '" and year = ' + str(year) + ' and matchup = "'
                                             + match_up + '" and field = "' + field + '";'))
    db.close()


def write_outcome_by_direction(player_id, p_uid, year, match_up, outcome_by_direction, player_type):
    db = DatabaseConnection(sandbox_mode=True)
    direction_totals = {}
    for direction, outcomes in outcome_by_direction.items():
        for outcome, total in outcomes.items():
            if direction in direction_totals:
                direction_totals[direction] += total
            else:
                direction_totals[direction] = total
        with ThreadPoolExecutor(os.cpu_count()) as executor:
            for outcome, total in outcomes.items():
                if len(db.read('select * from outcome_by_direction_' + player_type + ' where playerid = "' + player_id
                               + '" and year = ' + str(year) + ' and matchup = "' + match_up + '" and direction = "'
                               + direction + '";')) == 0:
                    executor.submit(db.write('insert into outcome_by_direction_' + player_type + ' (uid, playerid, '
                                             'year, matchup, direction, '
                                             + outcome.replace(' ', '_').replace(')', '').replace('(', '') + ', p_uid) '
                                             'values (default, "' + player_id + '", ' + str(year) + ', "' + match_up
                                             + '", "' + direction + '", ' + str(round(total/direction_totals[direction],
                                                                                      3)) + ', ' + str(p_uid) + ');'))
                else:
                    executor.submit(db.write('update outcome_by_direction_' + player_type + ' set '
                                             + outcome.replace(' ', '_').replace(')', '').replace('(', '') + ' = '
                                             + str(round(total/direction_totals[direction], 3)) + ' where playerid '
                                             '= "' + player_id + '" and year = ' + str(year) + ' and matchup = "'
                                             + match_up + '" and direction = "' + direction + '";'))
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


def calculate_pitch_locations(pitches):
    x_coordinates = []
    y_coordinates = []
    for pitch in pitches:
        x_coordinates.append(pitch[6])
        y_coordinates.append(pitch[7])
    if len(x_coordinates) > 1:
        return {'x_mean': round(stat.mean(x_coordinates), 3), 'x_stdev': round(stat.stdev(x_coordinates), 3),
                'y_mean': round(stat.mean(y_coordinates), 3), 'y_stdev': round(stat.stdev(y_coordinates), 3)}
    else:
        return {'x_mean': None, 'x_stdev': None, 'y_mean': None, 'y_stdev': None}


def pitch_in_zone(x, y, x_coordinates, y_coordinates):
    return x_coordinates['low'] < x < x_coordinates['high'] and y_coordinates['low'] < y < y_coordinates['high']


def strike_zone_coordinate(coordinate):
    try:
        with open(os.path.join('..', '..', 'background', 'strike_zone.json')) as strike_zone_file:
            strike_zone = json.load(strike_zone_file)
    except FileNotFoundError:
        with open(os.path.join('..', '..', '..', '..', 'background', 'strike_zone.json')) as strike_zone_file:
            strike_zone = json.load(strike_zone_file)
    return {'low': float(strike_zone.get(coordinate + '_low_strike')),
            'high': float(strike_zone.get(coordinate + '_high_strike'))}


def calculate_swing_rate(pitches):
    pitches_in_zone = []
    pitches_out_of_zone = []
    x_coordinates = strike_zone_coordinate('x')
    y_coordinates = strike_zone_coordinate('y')
    for pitch in pitches:
        if pitch_in_zone(pitch[6], pitch[7], x_coordinates, y_coordinates):
            pitches_in_zone.append(pitch[0])
        else:
            pitches_out_of_zone.append(pitch[0])
    try:
        swing_rate_in_zone = pitches_in_zone.count('swing') / len(pitches_in_zone)
    except ZeroDivisionError:
        swing_rate_in_zone = None
    try:
        swing_rate_out_of_zone = pitches_out_of_zone.count('swing') / len(pitches_out_of_zone)
    except ZeroDivisionError:
        swing_rate_out_of_zone = None
    return {'in_zone': swing_rate_in_zone, 'out_of_zone': swing_rate_out_of_zone}


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


def sort_outcomes_by_trajectory(pitches, match_up):
    trajectories = {'vr': {}, 'vl': {}}
    for pitch in pitches:
        if pitch[5][:2] == match_up[:2]:
            if pitch[11] not in trajectories[match_up[:2]]:
                trajectories[match_up[:2]][pitch[11]] = {}
            if pitch[10] not in trajectories[match_up[:2]][pitch[11]]:
                trajectories[match_up[:2]][pitch[11]][pitch[10]] = 0
            trajectories[match_up[:2]][pitch[11]][pitch[10]] += 1
    return trajectories[match_up[:2]]


def sort_outcomes_by_field(pitches, match_up):
    fields = {'vr': {}, 'vl': {}}
    for pitch in pitches:
        if pitch[5][:2] == match_up[:2]:
            if pitch[12] not in fields[match_up[:2]]:
                fields[match_up[:2]][pitch[12]] = {}
            if pitch[10] not in fields[match_up[:2]][pitch[12]]:
                fields[match_up[:2]][pitch[12]][pitch[10]] = 0
            fields[match_up[:2]][pitch[12]][pitch[10]] += 1
    return fields[match_up[:2]]


def sort_outcomes_by_direction(pitches, match_up):
    directions = {'vr': {}, 'vl': {}}
    for pitch in pitches:
        if pitch[5][:2] == match_up[:2]:
            if pitch[13] not in directions[match_up[:2]]:
                directions[match_up[:2]][pitch[13]] = {}
            if pitch[10] not in directions[match_up[:2]][pitch[13]]:
                directions[match_up[:2]][pitch[13]][pitch[10]] = 0
            directions[match_up[:2]][pitch[13]][pitch[10]] += 1
    return directions[match_up[:2]]


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


def this_batter_is_a_pitcher(p_uid):
    db = DatabaseConnection(sandbox_mode=True)
    if db.read('select primaryPosition from players where playerId = (select playerId from player_teams where '
               'pt_uniqueidentifier = (select pt_uniqueidentifier from player_batting where pb_uniqueidentifier = '
               + str(p_uid) + '));')[0][0] in ['SP', 'RP']:
        is_a_pitcher = True
    else:
        is_a_pitcher = False
    db.close()
    return is_a_pitcher


def accumulate_all_pitchers_batting_dict(pitch_type, pitches, match_up, count):
    global all_pitchers_batting_data
    increment_all_pitchers_pitch_count_batting_data_overall(match_up, count, pitch_type, len(pitches))
    pitch_type_outcomes = {}
    swing_rates = {}
    pitch_locations = {}
    trajectories = {}
    fields = {}
    directions = {}
    outcomes_by_trajectory = {}
    outcomes_by_field = {}
    outcomes_by_direction = {}
    for pitch in pitches:
        swing_take = pitch[0]
        outcome = pitch[2]
        trajectory = pitch[3]
        field = pitch[4]
        direction = pitch[5]
        x = pitch[6]
        y = pitch[7]
        pitch_type_outcomes = \
            accumulate_pitch_type_outcomes_individual(pitch_type_outcomes, match_up, count, pitch_type, outcome)
        swing_rates = accumulate_swing_rates_individual(swing_rates, match_up, count, pitch_type, swing_take, x, y)
        pitch_locations = accumulate_pitch_locations_individual(pitch_locations, pitch_type, match_up, count, x, y)
        trajectories = accumulate_trajectories_by_pitch_type_individual(trajectories, pitch_type, match_up, count,
                                                                        trajectory)
        fields = accumulate_fields_by_pitch_type_individual(fields, match_up, count, pitch_type, field)
        directions = accumulate_directions_by_pitch_type_individual(directions, match_up, count, pitch_type, direction)
        outcomes_by_trajectory = accumulate_outcomes_by_trajectory_individual(outcomes_by_trajectory, trajectory,
                                                                              outcome, match_up, count)
        outcomes_by_field = accumulate_outcomes_by_field_individual(outcomes_by_field, field, outcome, match_up, count)
        outcomes_by_direction = accumulate_outcomes_by_direction_individual(outcomes_by_direction, direction, outcome,
                                                                            match_up, count)
    accumulate_pitch_type_outcomes_overall(pitch_type_outcomes, match_up, count)
    accumulate_swing_rates_overall(swing_rates, match_up, count)
    accumulate_pitch_locations_overall(pitch_locations, pitch_type, match_up, count)
    accumulate_trajectories_overall(trajectories, match_up, count)
    accumulate_fields_overall(fields, match_up, count)
    accumulate_directions_overall(directions, match_up, count)
    accumulate_trajectories_by_outcomes_overall(outcomes_by_trajectory, match_up, count)
    accumulate_fields_by_outcomes_overall(outcomes_by_field, match_up, count)
    accumulate_directions_by_outcomes_overall(outcomes_by_direction, match_up, count)


def accumulate_pitch_type_outcomes_individual(pitch_type_outcomes, match_up, count, pitch_type, outcome):
    if match_up not in pitch_type_outcomes:
        pitch_type_outcomes[match_up] = {}
    if count not in pitch_type_outcomes[match_up]:
        pitch_type_outcomes[match_up][count] = {}
    if pitch_type not in pitch_type_outcomes[match_up][count]:
        pitch_type_outcomes[match_up][count][pitch_type] = {}
    if outcome not in pitch_type_outcomes[match_up][count][pitch_type]:
        pitch_type_outcomes[match_up][count][pitch_type][outcome] = 0
    pitch_type_outcomes[match_up][count][pitch_type][outcome] += 1
    return pitch_type_outcomes


def accumulate_swing_rates_individual(swing_rates, match_up, count, pitch_type, swing_take, x, y):
    if match_up not in swing_rates:
        swing_rates[match_up] = {}
    if count not in swing_rates[match_up]:
        swing_rates[match_up][count] = {True: {}, False: {}}
    if pitch_type not in swing_rates[match_up][count][True]:
            swing_rates[match_up][count][True][pitch_type] = []
    if pitch_type not in swing_rates[match_up][count][False]:
            swing_rates[match_up][count][False][pitch_type] = []
    swing_rates[match_up][count][pitch_in_zone(x, y, strike_zone_coordinate('x'), strike_zone_coordinate('y'))]\
        [pitch_type].append(swing_take)
    return swing_rates


def accumulate_pitch_locations_individual(pitch_locations, pitch_type, match_up, count, x, y):
    if match_up not in pitch_locations:
        pitch_locations[match_up] = {}
    if count not in pitch_locations[match_up]:
        pitch_locations[match_up][count] = {}
    if pitch_type not in pitch_locations[match_up][count]:
        pitch_locations[match_up][count][pitch_type] = {'x': [], 'y': []}
    pitch_locations[match_up][count][pitch_type]['x'].append(x)
    pitch_locations[match_up][count][pitch_type]['y'].append(y)
    return pitch_locations


def accumulate_trajectories_by_pitch_type_individual(trajectories, pitch_type, match_up, count, trajectory):
    if match_up not in trajectories:
        trajectories[match_up] = {}
    if count not in trajectories[match_up]:
        trajectories[match_up][count] = {}
    if pitch_type not in trajectories[match_up][count]:
        trajectories[match_up][count][pitch_type] = {}
    if trajectory not in trajectories[match_up][count][pitch_type]:
        trajectories[match_up][count][pitch_type][trajectory] = 0
    trajectories[match_up][count][pitch_type][trajectory] += 1
    return trajectories


def accumulate_fields_by_pitch_type_individual(fields, match_up, count, pitch_type, field):
    if match_up not in fields:
        fields[match_up] = {}
    if count not in fields[match_up]:
        fields[match_up][count] = {}
    if pitch_type not in fields[match_up][count]:
        fields[match_up][count][pitch_type] = {}
    if field not in fields[match_up][count][pitch_type]:
        fields[match_up][count][pitch_type][field] = 0
    fields[match_up][count][pitch_type][field] += 1
    return fields


def accumulate_directions_by_pitch_type_individual(directions, match_up, count, pitch_type, direction):
    if match_up not in directions:
        directions[match_up] = {}
    if count not in directions[match_up]:
        directions[match_up][count] = {}
    if pitch_type not in directions[match_up][count]:
        directions[match_up][count][pitch_type] = {}
    if direction not in directions[match_up][count][pitch_type]:
        directions[match_up][count][pitch_type][direction] = 0
    directions[match_up][count][pitch_type][direction] += 1
    return directions


def accumulate_outcomes_by_trajectory_individual(outcomes_by_trajectory, trajectory, outcome, match_up, count):
    if match_up not in outcomes_by_trajectory:
        outcomes_by_trajectory[match_up] = {}
    if count not in outcomes_by_trajectory[match_up]:
        outcomes_by_trajectory[match_up][count] = {}
    if trajectory not in outcomes_by_trajectory[match_up][count]:
        outcomes_by_trajectory[match_up][count][trajectory] = {}
    if outcome not in outcomes_by_trajectory[match_up][count][trajectory]:
        outcomes_by_trajectory[match_up][count][trajectory][outcome] = 0
    outcomes_by_trajectory[match_up][count][trajectory][outcome] += 1
    return outcomes_by_trajectory


def accumulate_outcomes_by_field_individual(outcomes_by_field, field, outcome, match_up, count):
    if match_up not in outcomes_by_field:
        outcomes_by_field[match_up] = {}
    if count not in outcomes_by_field[match_up]:
        outcomes_by_field[match_up][count] = {}
    if field not in outcomes_by_field[match_up][count]:
        outcomes_by_field[match_up][count][field] = {}
    if outcome not in outcomes_by_field[match_up][count][field]:
        outcomes_by_field[match_up][count][field][outcome] = 0
    outcomes_by_field[match_up][count][field][outcome] += 1
    return outcomes_by_field


def accumulate_outcomes_by_direction_individual(outcomes_by_direction, direction, outcome, match_up, count):
    if match_up not in outcomes_by_direction:
        outcomes_by_direction[match_up] = {}
    if count not in outcomes_by_direction[match_up]:
        outcomes_by_direction[match_up][count] = {}
    if direction not in outcomes_by_direction[match_up][count]:
        outcomes_by_direction[match_up][count][direction] = {}
    if outcome not in outcomes_by_direction[match_up][count][direction]:
        outcomes_by_direction[match_up][count][direction][outcome] = 0
    outcomes_by_direction[match_up][count][direction][outcome] += 1
    return outcomes_by_direction


def increment_all_pitchers_pitch_count_batting_data_overall(match_up, count, pitch_type, pitch_count):
    global all_pitchers_batting_data
    if match_up not in all_pitchers_batting_data['pitch_count_batting']:
        all_pitchers_batting_data['pitch_count_batting'][match_up] = {}
    if count not in all_pitchers_batting_data['pitch_count_batting'][match_up]:
        all_pitchers_batting_data['pitch_count_batting'][match_up][count] = {}
    if pitch_type not in all_pitchers_batting_data['pitch_count_batting'][match_up][count]:
        all_pitchers_batting_data['pitch_count_batting'][match_up][count][pitch_type] = 0
    all_pitchers_batting_data['pitch_count_batting'][match_up][count][pitch_type] += pitch_count


def increment_league_pitch_count_data(match_up, count, pitch_type, pitch_count):
    global league_pitching_data
    if match_up not in league_pitching_data['pitch_count_pitching']:
        league_pitching_data['pitch_count_pitching'][match_up] = {}
    if count not in league_pitching_data['pitch_count_pitching'][match_up]:
        league_pitching_data['pitch_count_pitching'][match_up][count] = {}
    if pitch_type not in league_pitching_data['pitch_count_pitching'][match_up][count]:
        league_pitching_data['pitch_count_pitching'][match_up][count][pitch_type] = 0
    league_pitching_data['pitch_count_pitching'][match_up][count][pitch_type] += pitch_count


def all_pitchers_pitch_usage_batting_data_overall():
    global all_pitchers_batting_data
    for match_up, count_data in all_pitchers_batting_data['pitch_count_batting'].items():
        if match_up not in all_pitchers_batting_data['pitch_usage_batting']:
            all_pitchers_batting_data['pitch_usage_batting'][match_up] = {}
        for count, pitch_data in count_data.items():
            total_pitches_for_this_count = 0
            if count not in all_pitchers_batting_data['pitch_usage_batting'][match_up]:
                all_pitchers_batting_data['pitch_usage_batting'][match_up][count] = {}
            for pitch_type, pitch_count in pitch_data.items():
                total_pitches_for_this_count += pitch_count
            for pitch_type, pitch_count in pitch_data.items():
                all_pitchers_batting_data['pitch_usage_batting'][match_up][count][pitch_type] = \
                    round(pitch_count/total_pitches_for_this_count, 3)


def league_pitch_usage_batting_data_overall():
    global league_pitching_data
    for match_up, count_data in league_pitching_data['pitch_count_pitching'].items():
        if match_up not in league_pitching_data['pitch_usage_pitching']:
            league_pitching_data['pitch_usage_pitching'][match_up] = {}
        for count, pitch_data in count_data.items():
            total_pitches_for_this_count = 0
            if count not in league_pitching_data['pitch_usage_pitching'][match_up]:
                league_pitching_data['pitch_usage_pitching'][match_up][count] = {}
            for pitch_type, pitch_count in pitch_data.items():
                total_pitches_for_this_count += pitch_count
            for pitch_type, pitch_count in pitch_data.items():
                league_pitching_data['pitch_usage_pitching'][match_up][count][pitch_type] = \
                    round(pitch_count/total_pitches_for_this_count, 3)


def accumulate_pitch_type_outcomes_overall(pitch_type_outcomes, match_up, count):
    global all_pitchers_batting_data
    if match_up not in all_pitchers_batting_data['outcomes_batting']:
        all_pitchers_batting_data['outcomes_batting'][match_up] = {}
    if count not in all_pitchers_batting_data['outcomes_batting'][match_up]:
        all_pitchers_batting_data['outcomes_batting'][match_up][count] = {}
    for pitch_type, outcomes in pitch_type_outcomes[match_up][count].items():
        if pitch_type not in all_pitchers_batting_data['outcomes_batting'][match_up][count]:
            all_pitchers_batting_data['outcomes_batting'][match_up][count][pitch_type] = \
                pitch_type_outcomes[match_up][count][pitch_type]
        else:
            for outcome, occurrences in outcomes. items():
                if outcome not in all_pitchers_batting_data['outcomes_batting'][match_up][count][pitch_type]:
                    all_pitchers_batting_data['outcomes_batting'][match_up][count][pitch_type][outcome] = 0
                all_pitchers_batting_data['outcomes_batting'][match_up][count][pitch_type][outcome] += occurrences


def accumulate_league_pitching_pitch_type_outcomes_overall(pitch_type_outcomes, match_up, count):
    global league_pitching_data
    if match_up not in league_pitching_data['outcomes_pitching']:
        league_pitching_data['outcomes_pitching'][match_up] = {}
    if count not in league_pitching_data['outcomes_pitching'][match_up]:
        league_pitching_data['outcomes_pitching'][match_up][count] = {}
    for pitch_type, outcomes in pitch_type_outcomes[match_up][count].items():
        if pitch_type not in league_pitching_data['outcomes_pitching'][match_up][count]:
            league_pitching_data['outcomes_pitching'][match_up][count][pitch_type] = \
                pitch_type_outcomes[match_up][count][pitch_type]
        else:
            for outcome, occurrences in outcomes. items():
                if outcome not in league_pitching_data['outcomes_pitching'][match_up][count][pitch_type]:
                    league_pitching_data['outcomes_pitching'][match_up][count][pitch_type][outcome] = 0
                league_pitching_data['outcomes_pitching'][match_up][count][pitch_type][outcome] += occurrences


def accumulate_swing_rates_overall(swing_rates, match_up, count):
    global all_pitchers_batting_data
    if match_up not in all_pitchers_batting_data['temp_swing_rate_batting']:
        all_pitchers_batting_data['temp_swing_rate_batting'][match_up] = {}
    if count not in all_pitchers_batting_data['temp_swing_rate_batting'][match_up]:
        all_pitchers_batting_data['temp_swing_rate_batting'][match_up][count] = {True: {}, False: {}}
    for boolean in [True, False]:
        for pitch_type, swings_takes in swing_rates[match_up][count][boolean].items():
            if pitch_type not in all_pitchers_batting_data['temp_swing_rate_batting'][match_up][count][boolean]:
                all_pitchers_batting_data['temp_swing_rate_batting'][match_up][count][boolean][pitch_type] = []
            all_pitchers_batting_data['temp_swing_rate_batting'][match_up][count][boolean][pitch_type] += \
                swing_rates[match_up][count][boolean][pitch_type]


def accumulate_league_pitching_swing_rates_overall(swing_rates, match_up, count):
    global league_pitching_data
    if match_up not in league_pitching_data['temp_swing_rate_pitching']:
        league_pitching_data['temp_swing_rate_pitching'][match_up] = {}
    if count not in league_pitching_data['temp_swing_rate_pitching'][match_up]:
        league_pitching_data['temp_swing_rate_pitching'][match_up][count] = {True: {}, False: {}}
    for boolean in [True, False]:
        for pitch_type, swings_takes in swing_rates[match_up][count][boolean].items():
            if pitch_type not in league_pitching_data['temp_swing_rate_pitching'][match_up][count][boolean]:
                league_pitching_data['temp_swing_rate_pitching'][match_up][count][boolean][pitch_type] = []
            league_pitching_data['temp_swing_rate_pitching'][match_up][count][boolean][pitch_type] += \
                swing_rates[match_up][count][boolean][pitch_type]


def determine_overall_swing_rates_in_and_out_of_zone(match_up, count):
    global all_pitchers_batting_data
    if match_up not in all_pitchers_batting_data['swing_rate_batting']:
        all_pitchers_batting_data['swing_rate_batting'][match_up] = {}
    if count not in all_pitchers_batting_data['swing_rate_batting'][match_up]:
        all_pitchers_batting_data['swing_rate_batting'][match_up][count] = {}
    try:
        for strike_ball, pitch_types in all_pitchers_batting_data['temp_swing_rate_batting'][match_up]\
                [count].items():
            if strike_ball not in all_pitchers_batting_data['swing_rate_batting'][match_up][count]:
                all_pitchers_batting_data['swing_rate_batting'][match_up][count][strike_ball] = {}
            for pitch_type, swings_takes in pitch_types.items():
                try:
                    all_pitchers_batting_data['swing_rate_batting'][match_up][count][strike_ball][pitch_type] = \
                        all_pitchers_batting_data['temp_swing_rate_batting'][match_up][count][strike_ball]\
                            [pitch_type].count('swing') / \
                        len(all_pitchers_batting_data['temp_swing_rate_batting'][match_up][count][strike_ball]
                            [pitch_type])
                except ZeroDivisionError:
                    all_pitchers_batting_data['swing_rate_batting'][match_up][count][strike_ball][pitch_type] = \
                        None
    except KeyError:
        all_pitchers_batting_data['swing_rate_batting'][match_up][count] = {True: {}, False: {}}


def determine_overall_league_pitching_swing_rates_in_and_out_of_zone(match_up, count):
    global league_pitching_data
    if match_up not in league_pitching_data['swing_rate_pitching']:
        league_pitching_data['swing_rate_pitching'][match_up] = {}
    if count not in league_pitching_data['swing_rate_pitching'][match_up]:
        league_pitching_data['swing_rate_pitching'][match_up][count] = {}
    try:
        for strike_ball, pitch_types in league_pitching_data['temp_swing_rate_pitching'][match_up]\
                [count].items():
            if strike_ball not in league_pitching_data['swing_rate_pitching'][match_up][count]:
                league_pitching_data['swing_rate_pitching'][match_up][count][strike_ball] = {}
            for pitch_type, swings_takes in pitch_types.items():
                try:
                    league_pitching_data['swing_rate_pitching'][match_up][count][strike_ball][pitch_type] = \
                        league_pitching_data['temp_swing_rate_pitching'][match_up][count][strike_ball]\
                            [pitch_type].count('swing') / \
                        len(league_pitching_data['temp_swing_rate_pitching'][match_up][count][strike_ball]
                            [pitch_type])
                except ZeroDivisionError:
                    league_pitching_data['swing_rate_pitching'][match_up][count][strike_ball][pitch_type] = \
                        None
    except KeyError:
        league_pitching_data['swing_rate_pitching'][match_up][count] = {True: {}, False: {}}


def accumulate_pitch_locations_overall(pitch_locations, pitch_type, match_up, count):
    global all_pitchers_batting_data
    if match_up not in all_pitchers_batting_data['temp_pitch_location_batting']:
        all_pitchers_batting_data['temp_pitch_location_batting'][match_up] = {}
    if count not in all_pitchers_batting_data['temp_pitch_location_batting'][match_up]:
        all_pitchers_batting_data['temp_pitch_location_batting'][match_up][count] = {}
    if pitch_type not in all_pitchers_batting_data['temp_pitch_location_batting'][match_up][count]:
        all_pitchers_batting_data['temp_pitch_location_batting'][match_up][count][pitch_type] = \
            {'x': [], 'y': []}
    for coordinate_type in ['x', 'y']:
        all_pitchers_batting_data['temp_pitch_location_batting'][match_up][count][pitch_type]\
            [coordinate_type] += pitch_locations[match_up][count][pitch_type][coordinate_type]


def accumulate_league_pitching_pitch_locations_overall(pitch_locations, pitch_type, match_up, count):
    global league_pitching_data
    if match_up not in league_pitching_data['temp_pitch_location_pitching']:
        league_pitching_data['temp_pitch_location_pitching'][match_up] = {}
    if count not in league_pitching_data['temp_pitch_location_pitching'][match_up]:
        league_pitching_data['temp_pitch_location_pitching'][match_up][count] = {}
    if pitch_type not in league_pitching_data['temp_pitch_location_pitching'][match_up][count]:
        league_pitching_data['temp_pitch_location_pitching'][match_up][count][pitch_type] = \
            {'x': [], 'y': []}
    for coordinate_type in ['x', 'y']:
        league_pitching_data['temp_pitch_location_pitching'][match_up][count][pitch_type]\
            [coordinate_type] += pitch_locations[match_up][count][pitch_type][coordinate_type]


def find_mean_and_stdev_of_pitch_locations(match_up, count, pitch_type):
    global all_pitchers_batting_data
    if match_up not in all_pitchers_batting_data['pitch_location_batting']:
        all_pitchers_batting_data['pitch_location_batting'][match_up] = {}
    if count not in all_pitchers_batting_data['pitch_location_batting'][match_up]:
        all_pitchers_batting_data['pitch_location_batting'][match_up][count] = {}
    if pitch_type not in all_pitchers_batting_data['pitch_location_batting'][match_up][count]:
        all_pitchers_batting_data['pitch_location_batting'][match_up][count][pitch_type] = {}
    for location in ['x', 'y']:
        try:
            all_pitchers_batting_data['pitch_location_batting'][match_up][count][pitch_type]\
                [location + '_mean'] = stat.mean(all_pitchers_batting_data['temp_pitch_location_batting']\
                                                                 [match_up][count][pitch_type][location])
            try:
                all_pitchers_batting_data['pitch_location_batting'][match_up][count][pitch_type]\
                    [location + '_stdev'] = stat.stdev(all_pitchers_batting_data['temp_pitch_location_batting']\
                                                                                [match_up][count][pitch_type][location])
            except stat.StatisticsError:  # there's one or zero data points. Cannot calculate standard deviation with < 2
                all_pitchers_batting_data['pitch_location_batting'][match_up][count][pitch_type]\
                    [location + '_stdev'] = 0
        except KeyError:  # given pitch_type is not present in all_pitchers_batting_data['temp_pitch_location_batting']
            all_pitchers_batting_data['pitch_location_batting'][match_up][count][pitch_type]\
                [location + '_mean'] = None
            all_pitchers_batting_data['pitch_location_batting'][match_up][count][pitch_type] \
                [location + '_stdev'] = None


def find_mean_and_stdev_of_league_pitching_pitch_locations(match_up, count, pitch_type):
    global league_pitching_data
    if match_up not in league_pitching_data['pitch_location_pitching']:
        league_pitching_data['pitch_location_pitching'][match_up] = {}
    if count not in league_pitching_data['pitch_location_pitching'][match_up]:
        league_pitching_data['pitch_location_pitching'][match_up][count] = {}
    if pitch_type not in league_pitching_data['pitch_location_pitching'][match_up][count]:
        league_pitching_data['pitch_location_pitching'][match_up][count][pitch_type] = {}
    for location in ['x', 'y']:
        try:
            league_pitching_data['pitch_location_pitching'][match_up][count][pitch_type]\
                [location + '_mean'] = stat.mean(league_pitching_data['temp_pitch_location_pitching']\
                                                                 [match_up][count][pitch_type][location])
            try:
                league_pitching_data['pitch_location_pitching'][match_up][count][pitch_type]\
                    [location + '_stdev'] = stat.stdev(league_pitching_data['temp_pitch_location_pitching']\
                                                                                [match_up][count][pitch_type][location])
            except stat.StatisticsError:  # there's one or zero data points. Cannot calculate standard deviation with < 2
                league_pitching_data['pitch_location_pitching'][match_up][count][pitch_type]\
                    [location + '_stdev'] = 0
        except KeyError:  # given pitch_type is not present in league_pitching_data['temp_pitch_location_batting']
            league_pitching_data['pitch_location_pitching'][match_up][count][pitch_type]\
                [location + '_mean'] = None
            league_pitching_data['pitch_location_pitching'][match_up][count][pitch_type] \
                [location + '_stdev'] = None


def accumulate_trajectories_overall(trajectory_by_pitch_type, match_up, count):
    global all_pitchers_batting_data
    if match_up not in all_pitchers_batting_data['trajectory_batting']:
        all_pitchers_batting_data['trajectory_batting'][match_up] = {}
    if count not in all_pitchers_batting_data['trajectory_batting'][match_up]:
        all_pitchers_batting_data['trajectory_batting'][match_up][count] = {}
    for pitch_type, trajectories in trajectory_by_pitch_type[match_up][count].items():
        if pitch_type not in all_pitchers_batting_data['trajectory_batting'][match_up][count]:
            all_pitchers_batting_data['trajectory_batting'][match_up][count][pitch_type] = \
                trajectory_by_pitch_type[match_up][count][pitch_type]
        else:
            for trajectory, occurrences in trajectory_by_pitch_type[match_up][count][pitch_type].items():
                if trajectory not in all_pitchers_batting_data['trajectory_batting'][match_up][count][pitch_type]:
                    all_pitchers_batting_data['trajectory_batting'][match_up][count][pitch_type][trajectory] = 0
                all_pitchers_batting_data['trajectory_batting'][match_up][count][pitch_type][trajectory] += \
                    trajectory_by_pitch_type[match_up][count][pitch_type][trajectory]


def accumulate_league_pitching_trajectories_overall(trajectory_by_pitch_type, match_up, count):
    global league_pitching_data
    if match_up not in league_pitching_data['trajectory_pitching']:
        league_pitching_data['trajectory_pitching'][match_up] = {}
    if count not in league_pitching_data['trajectory_pitching'][match_up]:
        league_pitching_data['trajectory_pitching'][match_up][count] = {}
    for pitch_type, trajectories in trajectory_by_pitch_type[match_up][count].items():
        if pitch_type not in league_pitching_data['trajectory_pitching'][match_up][count]:
            league_pitching_data['trajectory_pitching'][match_up][count][pitch_type] = \
                trajectory_by_pitch_type[match_up][count][pitch_type]
        else:
            for trajectory, occurrences in trajectory_by_pitch_type[match_up][count][pitch_type].items():
                if trajectory not in league_pitching_data['trajectory_pitching'][match_up][count][pitch_type]:
                    league_pitching_data['trajectory_pitching'][match_up][count][pitch_type][trajectory] = 0
                league_pitching_data['trajectory_pitching'][match_up][count][pitch_type][trajectory] += \
                    trajectory_by_pitch_type[match_up][count][pitch_type][trajectory]


def accumulate_fields_overall(fields, match_up, count):
    global all_pitchers_batting_data
    if match_up not in all_pitchers_batting_data['field_batting']:
        all_pitchers_batting_data['field_batting'][match_up] = {}
    if count not in all_pitchers_batting_data['field_batting'][match_up]:
        all_pitchers_batting_data['field_batting'][match_up][count] = {}
    for pitch_type, trajectories in fields[match_up][count].items():
        if pitch_type not in all_pitchers_batting_data['field_batting'][match_up][count]:
            all_pitchers_batting_data['field_batting'][match_up][count][pitch_type] = \
                fields[match_up][count][pitch_type]
        else:
            for trajectory, occurrences in fields[match_up][count][pitch_type].items():
                if trajectory not in all_pitchers_batting_data['field_batting'][match_up][count][pitch_type]:
                    all_pitchers_batting_data['field_batting'][match_up][count][pitch_type][trajectory] = 0
                all_pitchers_batting_data['field_batting'][match_up][count][pitch_type][trajectory] += \
                    fields[match_up][count][pitch_type][trajectory]


def accumulate_league_pitching_fields_overall(fields, match_up, count):
    global league_pitching_data
    if match_up not in league_pitching_data['field_pitching']:
        league_pitching_data['field_pitching'][match_up] = {}
    if count not in league_pitching_data['field_pitching'][match_up]:
        league_pitching_data['field_pitching'][match_up][count] = {}
    for pitch_type, trajectories in fields[match_up][count].items():
        if pitch_type not in league_pitching_data['field_pitching'][match_up][count]:
            league_pitching_data['field_pitching'][match_up][count][pitch_type] = \
                fields[match_up][count][pitch_type]
        else:
            for trajectory, occurrences in fields[match_up][count][pitch_type].items():
                if trajectory not in league_pitching_data['field_pitching'][match_up][count][pitch_type]:
                    league_pitching_data['field_pitching'][match_up][count][pitch_type][trajectory] = 0
                league_pitching_data['field_pitching'][match_up][count][pitch_type][trajectory] += \
                    fields[match_up][count][pitch_type][trajectory]


def accumulate_directions_overall(directions, match_up, count):
    global all_pitchers_batting_data
    if match_up not in all_pitchers_batting_data['direction_batting']:
        all_pitchers_batting_data['direction_batting'][match_up] = {}
    if count not in all_pitchers_batting_data['direction_batting'][match_up]:
        all_pitchers_batting_data['direction_batting'][match_up][count] = {}
    for pitch_type, trajectories in directions[match_up][count].items():
        if pitch_type not in all_pitchers_batting_data['direction_batting'][match_up][count]:
            all_pitchers_batting_data['direction_batting'][match_up][count][pitch_type] = \
                directions[match_up][count][pitch_type]
        else:
            for trajectory, occurrences in directions[match_up][count][pitch_type].items():
                if trajectory not in all_pitchers_batting_data['direction_batting'][match_up][count][pitch_type]:
                    all_pitchers_batting_data['direction_batting'][match_up][count][pitch_type][trajectory] = 0
                all_pitchers_batting_data['direction_batting'][match_up][count][pitch_type][trajectory] += \
                    directions[match_up][count][pitch_type][trajectory]


def accumulate_league_pitching_directions_overall(directions, match_up, count):
    global league_pitching_data
    if match_up not in league_pitching_data['direction_pitching']:
        league_pitching_data['direction_pitching'][match_up] = {}
    if count not in league_pitching_data['direction_pitching'][match_up]:
        league_pitching_data['direction_pitching'][match_up][count] = {}
    for pitch_type, trajectories in directions[match_up][count].items():
        if pitch_type not in league_pitching_data['direction_pitching'][match_up][count]:
            league_pitching_data['direction_pitching'][match_up][count][pitch_type] = \
                directions[match_up][count][pitch_type]
        else:
            for trajectory, occurrences in directions[match_up][count][pitch_type].items():
                if trajectory not in league_pitching_data['direction_pitching'][match_up][count][pitch_type]:
                    league_pitching_data['direction_pitching'][match_up][count][pitch_type][trajectory] = 0
                league_pitching_data['direction_pitching'][match_up][count][pitch_type][trajectory] += \
                    directions[match_up][count][pitch_type][trajectory]


def accumulate_trajectories_by_outcomes_overall(outcome_by_trajectory, match_up, count):
    global all_pitchers_batting_data
    if match_up not in all_pitchers_batting_data['outcome_by_trajectory_batting']:
        all_pitchers_batting_data['outcome_by_trajectory_batting'][match_up] = {}
    if count not in all_pitchers_batting_data['outcome_by_trajectory_batting'][match_up]:
        all_pitchers_batting_data['outcome_by_trajectory_batting'][match_up][count] = {}
    for trajectory, outcomes in outcome_by_trajectory[match_up][count].items():
        if trajectory not in all_pitchers_batting_data['outcome_by_trajectory_batting'][match_up][count]:
            all_pitchers_batting_data['outcome_by_trajectory_batting'][match_up][count][trajectory] = \
                outcome_by_trajectory[match_up][count][trajectory]
        else:
            for outcome, occurrences in outcomes.items():
                if outcome not in all_pitchers_batting_data['outcome_by_trajectory_batting'][match_up][count]\
                        [trajectory]:
                    all_pitchers_batting_data['outcome_by_trajectory_batting'][match_up][count][trajectory]\
                        [outcome] = 0
                all_pitchers_batting_data['outcome_by_trajectory_batting'][match_up][count][trajectory]\
                    [outcome] += outcome_by_trajectory[match_up][count][trajectory][outcome]


def accumulate_league_pitching_trajectories_by_outcomes_overall(outcome_by_trajectory, match_up, count):
    global league_pitching_data
    if match_up not in league_pitching_data['outcome_by_trajectory_pitching']:
        league_pitching_data['outcome_by_trajectory_pitching'][match_up] = {}
    if count not in league_pitching_data['outcome_by_trajectory_pitching'][match_up]:
        league_pitching_data['outcome_by_trajectory_pitching'][match_up][count] = {}
    for trajectory, outcomes in outcome_by_trajectory[match_up][count].items():
        if trajectory not in league_pitching_data['outcome_by_trajectory_pitching'][match_up][count]:
            league_pitching_data['outcome_by_trajectory_pitching'][match_up][count][trajectory] = \
                outcome_by_trajectory[match_up][count][trajectory]
        else:
            for outcome, occurrences in outcomes.items():
                if outcome not in league_pitching_data['outcome_by_trajectory_pitching'][match_up][count]\
                        [trajectory]:
                    league_pitching_data['outcome_by_trajectory_pitching'][match_up][count][trajectory]\
                        [outcome] = 0
                league_pitching_data['outcome_by_trajectory_pitching'][match_up][count][trajectory]\
                    [outcome] += outcome_by_trajectory[match_up][count][trajectory][outcome]


def accumulate_fields_by_outcomes_overall(fields_by_outcome, match_up, count):
    global all_pitchers_batting_data
    if match_up not in all_pitchers_batting_data['outcome_by_field_batting']:
        all_pitchers_batting_data['outcome_by_field_batting'][match_up] = {}
    if count not in all_pitchers_batting_data['outcome_by_field_batting'][match_up]:
        all_pitchers_batting_data['outcome_by_field_batting'][match_up][count] = {}
    for field, outcomes in fields_by_outcome[match_up][count].items():
        if field not in all_pitchers_batting_data['outcome_by_field_batting'][match_up][count]:
            all_pitchers_batting_data['outcome_by_field_batting'][match_up][count][field] = \
                fields_by_outcome[match_up][count][field]
        else:
            for outcome, occurrences in outcomes.items():
                if outcome not in all_pitchers_batting_data['outcome_by_field_batting'][match_up][count]\
                        [field]:
                    all_pitchers_batting_data['outcome_by_field_batting'][match_up][count][field]\
                        [outcome] = 0
                all_pitchers_batting_data['outcome_by_field_batting'][match_up][count][field]\
                    [outcome] += fields_by_outcome[match_up][count][field][outcome]


def accumulate_league_pitching_fields_by_outcomes_overall(fields_by_outcome, match_up, count):
    global league_pitching_data
    if match_up not in league_pitching_data['outcome_by_field_pitching']:
        league_pitching_data['outcome_by_field_pitching'][match_up] = {}
    if count not in league_pitching_data['outcome_by_field_pitching'][match_up]:
        league_pitching_data['outcome_by_field_pitching'][match_up][count] = {}
    for field, outcomes in fields_by_outcome[match_up][count].items():
        if field not in league_pitching_data['outcome_by_field_pitching'][match_up][count]:
            league_pitching_data['outcome_by_field_pitching'][match_up][count][field] = \
                fields_by_outcome[match_up][count][field]
        else:
            for outcome, occurrences in outcomes.items():
                if outcome not in league_pitching_data['outcome_by_field_pitching'][match_up][count]\
                        [field]:
                    league_pitching_data['outcome_by_field_pitching'][match_up][count][field]\
                        [outcome] = 0
                league_pitching_data['outcome_by_field_pitching'][match_up][count][field]\
                    [outcome] += fields_by_outcome[match_up][count][field][outcome]


def accumulate_directions_by_outcomes_overall(directions_by_outcome, match_up, count):
    global all_pitchers_batting_data
    if match_up not in all_pitchers_batting_data['outcome_by_direction_batting']:
        all_pitchers_batting_data['outcome_by_direction_batting'][match_up] = {}
    if count not in all_pitchers_batting_data['outcome_by_direction_batting'][match_up]:
        all_pitchers_batting_data['outcome_by_direction_batting'][match_up][count] = {}
    for direction, outcomes in directions_by_outcome[match_up][count].items():
        if direction not in all_pitchers_batting_data['outcome_by_direction_batting'][match_up][count]:
            all_pitchers_batting_data['outcome_by_direction_batting'][match_up][count][direction] = \
                directions_by_outcome[match_up][count][direction]
        else:
            for outcome, occurrences in outcomes.items():
                if outcome not in all_pitchers_batting_data['outcome_by_direction_batting'][match_up][count]\
                        [direction]:
                    all_pitchers_batting_data['outcome_by_direction_batting'][match_up][count][direction]\
                        [outcome] = 0
                all_pitchers_batting_data['outcome_by_direction_batting'][match_up][count][direction]\
                    [outcome] += directions_by_outcome[match_up][count][direction][outcome]


def accumulate_league_pitching_directions_by_outcomes_overall(directions_by_outcome, match_up, count):
    global league_pitching_data
    if match_up not in league_pitching_data['outcome_by_direction_pitching']:
        league_pitching_data['outcome_by_direction_pitching'][match_up] = {}
    if count not in league_pitching_data['outcome_by_direction_pitching'][match_up]:
        league_pitching_data['outcome_by_direction_pitching'][match_up][count] = {}
    for direction, outcomes in directions_by_outcome[match_up][count].items():
        if direction not in league_pitching_data['outcome_by_direction_pitching'][match_up][count]:
            league_pitching_data['outcome_by_direction_pitching'][match_up][count][direction] = \
                directions_by_outcome[match_up][count][direction]
        else:
            for outcome, occurrences in outcomes.items():
                if outcome not in league_pitching_data['outcome_by_direction_pitching'][match_up][count]\
                        [direction]:
                    league_pitching_data['outcome_by_direction_pitching'][match_up][count][direction]\
                        [outcome] = 0
                league_pitching_data['outcome_by_direction_pitching'][match_up][count][direction]\
                    [outcome] += directions_by_outcome[match_up][count][direction][outcome]


def round_up_and_write_all_pitchers_batting_stats(year):
    global all_pitchers_batting_data
    db = PitchFXDatabaseConnection(sandbox_mode=True)
    pitch_types = db.read('select pitch_type from batter_pitches where year = ' + str(year) + ' group by pitch_type;')
    db.close()
    all_pitchers_pitch_usage_batting_data_overall()
    for match_up in ['vr', 'vl']:
        for ball in range(4):
            for strike in range(3):
                count = str(ball) + '-' + str(strike)
                for pitch_type in pitch_types:
                    determine_overall_swing_rates_in_and_out_of_zone(match_up, count)
                    find_mean_and_stdev_of_pitch_locations(match_up, count, pitch_type[0])
    del all_pitchers_batting_data['temp_pitch_location_batting']
    del all_pitchers_batting_data['temp_swing_rate_batting']
    write_all_pitchers_batting(year)


def write_all_pitchers_batting(year):
    start_time = time.time()
    logger.log('\tWriting all pitchers\' batting stats.')
    global all_pitchers_batting_data
    db = DatabaseConnection(sandbox_mode=True)
    year_info_dict = literal_eval(db.read('select year_info from years where year = ' + str(year) + ';')[0][0])
    year_info_dict['pitchers_batting_stats'] = all_pitchers_batting_data
    db.write('update years set year_info = "' + str(year_info_dict) + '" where year = "' + str(year) + '";')
    db.close()
    logger.log('\t\tTime = ' + time_converter(time.time() - start_time))


def round_up_and_write_league_pitching_stats(year):
    global league_pitching_data
    db = PitchFXDatabaseConnection(sandbox_mode=True)
    pitch_types = db.read('select pitch_type from batter_pitches where year = ' + str(year) + ' group by pitch_type;')
    db.close()
    league_pitch_usage_batting_data_overall()
    for match_up in ['vr', 'vl']:
        for ball in range(4):
            for strike in range(3):
                count = str(ball) + '-' + str(strike)
                for pitch_type in pitch_types:
                    determine_overall_league_pitching_swing_rates_in_and_out_of_zone(match_up, count)
                    find_mean_and_stdev_of_league_pitching_pitch_locations(match_up, count, pitch_type[0])
    del league_pitching_data['temp_pitch_location_pitching']
    del league_pitching_data['temp_swing_rate_pitching']
    write_league_pitching_stats(year)


def write_league_pitching_stats(year):
    start_time = time.time()
    logger.log('\tWriting league pitching stats.')
    global league_pitching_data
    db = DatabaseConnection(sandbox_mode=True)
    year_info_dict = literal_eval(db.read('select year_info from years where year = ' + str(year) + ';')[0][0])
    year_info_dict['league_pitching_stats'] = league_pitching_data
    db.write('update years set year_info = "' + str(year_info_dict) + '" where year = "' + str(year) + '";')
    db.close()
    logger.log('\t\tTime = ' + time_converter(time.time() - start_time))


def accumulate_league_pitch_data(pitch_type, pitches, match_up, count):
    global league_pitching_data
    increment_league_pitch_count_data(match_up, count, pitch_type, len(pitches))
    pitch_type_outcomes = {}
    swing_rates = {}
    pitch_locations = {}
    trajectories = {}
    fields = {}
    directions = {}
    outcomes_by_trajectory = {}
    outcomes_by_field = {}
    outcomes_by_direction = {}
    for pitch in pitches:
        swing_take = pitch[0]
        outcome = pitch[2]
        trajectory = pitch[3]
        field = pitch[4]
        direction = pitch[5]
        x = pitch[6]
        y = pitch[7]
        pitch_type_outcomes = \
            accumulate_pitch_type_outcomes_individual(pitch_type_outcomes, match_up, count, pitch_type, outcome)
        swing_rates = accumulate_swing_rates_individual(swing_rates, match_up, count, pitch_type, swing_take, x, y)
        pitch_locations = accumulate_pitch_locations_individual(pitch_locations, pitch_type, match_up, count, x, y)
        trajectories = accumulate_trajectories_by_pitch_type_individual(trajectories, pitch_type, match_up, count,
                                                                        trajectory)
        fields = accumulate_fields_by_pitch_type_individual(fields, match_up, count, pitch_type, field)
        directions = accumulate_directions_by_pitch_type_individual(directions, match_up, count, pitch_type, direction)
        outcomes_by_trajectory = accumulate_outcomes_by_trajectory_individual(outcomes_by_trajectory, trajectory,
                                                                              outcome, match_up, count)
        outcomes_by_field = accumulate_outcomes_by_field_individual(outcomes_by_field, field, outcome, match_up, count)
        outcomes_by_direction = accumulate_outcomes_by_direction_individual(outcomes_by_direction, direction, outcome,
                                                                            match_up, count)
    accumulate_league_pitching_pitch_type_outcomes_overall(pitch_type_outcomes, match_up, count)
    accumulate_league_pitching_swing_rates_overall(swing_rates, match_up, count)
    accumulate_league_pitching_pitch_locations_overall(pitch_locations, pitch_type, match_up, count)
    accumulate_league_pitching_trajectories_overall(trajectories, match_up, count)
    accumulate_league_pitching_fields_overall(fields, match_up, count)
    accumulate_league_pitching_directions_overall(directions, match_up, count)
    accumulate_league_pitching_trajectories_by_outcomes_overall(outcomes_by_trajectory, match_up, count)
    accumulate_league_pitching_fields_by_outcomes_overall(outcomes_by_field, match_up, count)
    accumulate_league_pitching_directions_by_outcomes_overall(outcomes_by_direction, match_up, count)


# aggregate_pitch_fx(2017)
# aggregate(2017, None, None, 'colege01', 'pitching')
# aggregate(2017, None, None, 'kershcl01', 'pitching')
# aggregate(2017, None, None, 'scherma01', 'batting')
# aggregate(2017, None, None, 'strasst01', 'batting')
