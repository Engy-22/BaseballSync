import os
from utilities.connections.baseball_data_connection import DatabaseConnection
from utilities.connections.pitchers_pitch_fx_connection import PitcherPitchFXDatabaseConnection
from utilities.connections.batters_pitch_fx_connection import BatterPitchFXDatabaseConnection
from utilities.logger import Logger
import time
import datetime
from concurrent.futures import ThreadPoolExecutor
from utilities.time_converter import time_converter

logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\logs\\import_data\\"
                "aggregate_pitch_fx_data.log")


def aggregate_pitch_fx_data(year, driver_logger, sandbox_mode):
    print('Aggregating pitch fx data')
    driver_logger.log("\tAggregating pitch fx data")
    start_time = time.time()
    logger.log("Aggregating pitch fx data for " + str(year) + ' || Timestamp: ' + datetime.datetime.today().
               strftime('%Y-%m-%d %H:%M:%S'))
    aggregate_and_write(year, 'pitching', PitcherPitchFXDatabaseConnection, sandbox_mode)
    aggregate_and_write(year, 'batting', BatterPitchFXDatabaseConnection, sandbox_mode)
    total_time = time_converter(time.time() - start_time)
    logger.log("Done aggregating " + str(year) + " pitch fx data: Time = " + total_time)
    driver_logger.log("\t\tTime = " + total_time)


def aggregate_and_write(year, player_type, db_connection, sandbox_mode):
    pitcher_time = time.time()
    logger.log("\tAggregating " + player_type + " data and writing to database")
    db = DatabaseConnection(sandbox_mode)
    players = set(db.read('select playerid from ' + player_type[:-3] + 'er_pitches'))
    print(len(players))
    db.close()
    with ThreadPoolExecutor(os.cpu_count()) as executor:
        for player_id in players:
            executor.submit(aggregate, year, player_id[0], player_type, sandbox_mode)
    logger.log("\tDone aggregating and writing" + player_type + " data: Time = " + str(time.time() - pitcher_time))
    new_db = db_connection()
    new_db.close()


def aggregate(year, player_id, player_type, sandbox_mode):
    print(player_id)
    table = player_type[:-3] + 'er_pitches'
    matchups = ['vr', 'vl']
    opponent = 'hb' if player_type == 'pitching' else 'hp'
    balls = [ball for ball in range(4)]
    strikes = [strike for strike in range(3)]
    pitches_length_pitch_type = {}
    pitches_length_swing_take_and_ball_strike = {}
    pitch_types_dict = {}
    swings_by_pitch_dict = {}
    strikes_by_pitch_dict = {}
    db = DatabaseConnection(sandbox_mode)
    temp_team_id = db.read('select teamid from player_teams where playerid = "' + player_id + '";')
    if len(temp_team_id) > 1:
        team_id = 'TOT'
    else:
        team_id = temp_team_id[0][0]
    p_uid = db.read('select p' + player_type[0] + '_uniqueidentifier from player_' + player_type + ' where year = '
                    + str(year) + ' and pt_uniqueidentifier = (select pt_uniqueidentifier from player_teams where '
                    'playerid = "' + player_id + '" and teamid = "' + team_id + '")' + ';')[0][0]
    for matchup in matchups:
        pitches_length_pitch_type[matchup] = {}
        pitches_length_swing_take_and_ball_strike[matchup] = {}
        pitch_types_dict[matchup] = {}
        swings_by_pitch_dict[matchup] = {}
        strikes_by_pitch_dict[matchup] = {}
        for ball in balls:
            for strike in strikes:
                pitch_types_dict[matchup][str(ball) + str(strike)] = {}
                swings_by_pitch_dict[matchup][str(ball) + str(strike)] = {}
                strikes_by_pitch_dict[matchup][str(ball) + str(strike)] = {}
                bulk_query = 'from ' + table + ' where playerid = "' + player_id + '" and year = ' + str(year) + ' and'\
                             ' matchup = "' + matchup + opponent + '" and count="' + str(ball) + '-' + str(strike) + '"'
                pitch_types = db.read('select pitch_type ' + bulk_query + ';')
                pitches_length_pitch_type[matchup][str(ball) + str(strike)] = len(pitch_types)
                pitches_length_swing_take_and_ball_strike[matchup][str(ball) + str(strike)] = {}
                for pitch_type in pitch_types:
                    if pitch_type in pitch_types_dict:
                        pitch_types_dict[matchup][str(ball) + str(strike)][pitch_type[0]] += 1
                        pitches_length_swing_take_and_ball_strike[matchup][str(ball) + str(strike)][pitch_type[0]] += 1
                    else:
                        pitch_types_dict[matchup][str(ball) + str(strike)][pitch_type[0]] = 1
                        swings_by_pitch_dict[matchup][str(ball) + str(strike)][pitch_type[0]] = 0
                        strikes_by_pitch_dict[matchup][str(ball) + str(strike)][pitch_type[0]] = 0
                        pitches_length_swing_take_and_ball_strike[matchup][str(ball) + str(strike)][pitch_type[0]] = 1
                for pitch_type in set(pitch_types):
                    bulk_query += ' and pitch_type = "' + pitch_type[0] + '"'
                    for swing_take in set(db.read('select swing_take ' + bulk_query + ';')):
                        if swing_take[0] == 'swing':
                            swings_by_pitch_dict[matchup][str(ball) + str(strike)][pitch_type[0]] += 1
                        bulk_query += ' and swing_take = "' + swing_take[0] + '"'
                        for ball_strike in set(db.read('select ball_strike ' + bulk_query + ';')):
                            if ball_strike[0] == 'strike':
                                strikes_by_pitch_dict[matchup][str(ball) + str(strike)][pitch_type[0]] += 1
                            bulk_query += ' and ball_strike = "' + ball_strike[0] + '"'
                            for outcome in set(db.read('select outcome ' + bulk_query + ';')):
                                bulk_query += ' and outcome = "' + outcome[0] + '"'
                                for trajectory in set(db.read('select trajectory ' + bulk_query + ';')):
                                    bulk_query += ' and trajectory = "' + trajectory[0] + '"'
                                    for field in set(db.read('select field ' + bulk_query + ';')):
                                        bulk_query += ' and field = "' + field[0] + '"'
                                        for directions in set(db.read('select direction ' + bulk_query + ';')):
                                            count = 0
                                            for direction in directions:
                                                count += 1
    write_pitch_usage(player_id, p_uid, year, pitch_types_dict, player_type, pitches_length_pitch_type, sandbox_mode)
    write_swing_take_by_pitch(player_id, p_uid, year, pitch_types_dict, swings_by_pitch_dict, player_type,
                              pitches_length_swing_take_and_ball_strike, sandbox_mode)
    write_ball_strike_by_pitch(player_id, p_uid, year, pitch_types_dict, strikes_by_pitch_dict, player_type,
                               pitches_length_swing_take_and_ball_strike, sandbox_mode)
    db.close()


def write_pitch_usage(player_id, p_uid, year, pitch_type_dict, player_type, length, sandbox_mode):
    print(pitch_type_dict)
    if player_type == 'pitching':
        db = PitcherPitchFXDatabaseConnection(sandbox_mode)
    else:
        db = BatterPitchFXDatabaseConnection(sandbox_mode)
    for matchup, counts in pitch_type_dict.items():
        print(counts)
        for count, pitch_types in counts.items():
            fields = ''
            values = ''
            for pitch, total in pitch_types.items():
                fields += ', ' + pitch
                values += ', ' + str(round(total/length[matchup][count], 3))
            db.write('insert into ' + matchup + '_' + count + '_pitch_type (uid, playerid, year' + fields + ', p_uid)'
                     ' values (default, "' + player_id + '", ' + str(year) + values + ', ' + str(p_uid) + ');')
    db.close()


def write_swing_take_by_pitch(player_id, p_uid, year, pitch_type_dict, swing_take_dict, player_type, length,
                              sandbox_mode):
    if player_type == 'pitching':
        db = PitcherPitchFXDatabaseConnection(sandbox_mode)
    else:
        db = BatterPitchFXDatabaseConnection(sandbox_mode)
    for matchup, counts in pitch_type_dict.items():
        for count, pitch_types in counts.items():
            fields = ''
            values = ''
            for pitch, total in pitch_types.items():
                fields += ', ' + pitch
                values += ', ' + str(round(swing_take_dict[matchup][count]/length[matchup][count][pitch], 3))
            db.write('insert into ' + matchup + '_' + count + '_swing_take (uid, playerid, year' + fields + ', p_uid)'
                     ' values (default, "' + player_id + '", ' + str(year) + values + ', ' + str(p_uid) + ');')
    db.close()


def write_ball_strike_by_pitch(player_id, p_uid, year, pitch_type_dict, ball_strike_dict, player_type, length,
                               sandbox_mode):
    if player_type == 'pitching':
        db = PitcherPitchFXDatabaseConnection(sandbox_mode)
    else:
        db = BatterPitchFXDatabaseConnection(sandbox_mode)
    for matchup, counts in pitch_type_dict.items():
        for count, pitch_types in counts.items():
            fields = ''
            values = ''
            for pitch, total in pitch_types.items():
                fields += ', ' + pitch
                values += ', ' + str(round(ball_strike_dict[matchup][count] / length[matchup][count][pitch], 3))
            db.write('insert into ' + matchup + '_' + count + '_ball_strike (uid, playerid, year' + fields + ', p_uid)'
                     ' values (default, "' + player_id + '", ' + str(year) + values + ', ' + str(p_uid) + ');')
    db.close()


aggregate_pitch_fx_data(2009, Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\"
                                     "dump.log"), False)
