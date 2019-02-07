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
    aggregate_and_write(year, 'pitching', sandbox_mode)
    # aggregate_and_write(year, 'batting', sandbox_mode)
    total_time = time_converter(time.time() - start_time)
    logger.log("Done aggregating " + str(year) + " pitch fx data: Time = " + total_time + '\n\n')
    driver_logger.log("\t\tTime = " + total_time)


def aggregate_and_write(year, player_type, sandbox_mode):
    pitcher_time = time.time()
    logger.log("\tAggregating " + player_type + " data and writing to database")
    db = DatabaseConnection(sandbox_mode)
    players = set(db.read('select playerid from ' + player_type[:-3] + 'er_pitches;'))
    db.close()
    for player_id in players:
        aggregate(year, player_id[0], player_type, sandbox_mode)
        break
    logger.log("\tDone aggregating and writing " + player_type + " data: Time = "
               + time_converter(time.time() - pitcher_time))


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
    for matchup in matchups:
        print(matchup)
        pitches_length_pitch_type[matchup] = {}
        pitches_length_swing_take_and_ball_strike[matchup] = {}
        pitch_types_dict[matchup] = {}
        swings_by_pitch_dict[matchup] = {}
        strikes_by_pitch_dict[matchup] = {}
        for ball in balls:
            for strike in strikes:
                pitch_types_dict[matchup][str(ball)+str(strike)] = {}
                swings_by_pitch_dict[matchup][str(ball)+str(strike)] = {}
                strikes_by_pitch_dict[matchup][str(ball)+str(strike)] = {}
                bulk_query = 'from ' + table + ' where playerid = "' + player_id + '" and year = ' + str(year) + ' and'\
                             ' matchup = "' + matchup + opponent + '" and count="' + str(ball) + '-' + str(strike) + '"'
                pitch_types = db.read('select pitch_type ' + bulk_query + ';')
                pitches_length_pitch_type[matchup][str(ball)+str(strike)] = len(pitch_types)
                pitches_length_swing_take_and_ball_strike[matchup][str(ball)+str(strike)] = {}
                for pitch_type in pitch_types:
                    if pitch_type[0] in pitch_types_dict[matchup][str(ball)+str(strike)]:
                        pitch_types_dict[matchup][str(ball)+str(strike)][pitch_type[0]] += 1
                        pitches_length_swing_take_and_ball_strike[matchup][str(ball)+str(strike)][pitch_type[0]] += 1
                    else:
                        pitch_types_dict[matchup][str(ball)+str(strike)][pitch_type[0]] = 1
                        pitches_length_swing_take_and_ball_strike[matchup][str(ball)+str(strike)][pitch_type[0]] = 1
                for pitch_type in set(pitch_types):
                    swings_by_pitch_dict[matchup][str(ball)+str(strike)][pitch_type[0]] = 0
                    strikes_by_pitch_dict[matchup][str(ball)+str(strike)][pitch_type[0]] = 0
                    temp_bulk_query = bulk_query + ' and pitch_type = "' + pitch_type[0] + '"'
                    for swing_take in db.read('select swing_take ' + temp_bulk_query + ';'):
                        if swing_take[0] == 'swing':
                            swings_by_pitch_dict[matchup][str(ball)+str(strike)][pitch_type[0]] += 1
                        temp_bulk_query += ' and swing_take = "' + swing_take[0] + '"'
                        for ball_strike in db.read('select ball_strike ' + temp_bulk_query + ';'):
                            if ball_strike[0] == 'strike':
                                strikes_by_pitch_dict[matchup][str(ball)+str(strike)][pitch_type[0]] += 1
                            temp_bulk_query += ' and ball_strike = "' + ball_strike[0] + '"'
                            for outcome in db.read('select outcome ' + temp_bulk_query + ';'):
                                temp_bulk_query += ' and outcome = "' + outcome[0] + '"'
                                for trajectory in db.read('select trajectory ' + temp_bulk_query + ';'):
                                    temp_bulk_query += ' and trajectory = "' + trajectory[0] + '"'
                                    for field in db.read('select field ' + temp_bulk_query + ';'):
                                        temp_bulk_query += ' and field = "' + field[0] + '"'
                                        for directions in db.read('select direction ' + temp_bulk_query + ';'):
                                            count = 0
                                            for direction in directions:
                                                count += 1
    # write_pitch_usage(player_id, p_uid, year, pitch_types_dict, player_type, pitches_length_pitch_type, sandbox_mode)
    write_swing_take_by_pitch(player_id, p_uid, year, swings_by_pitch_dict, player_type,
                              pitches_length_swing_take_and_ball_strike, sandbox_mode)
    write_ball_strike_by_pitch(player_id, p_uid, year, strikes_by_pitch_dict, player_type,
                               pitches_length_swing_take_and_ball_strike, sandbox_mode)
    db.close()


def write_pitch_usage(player_id, p_uid, year, pitch_type_dict, player_type, length, sandbox_mode):
    if player_type == 'pitching':
        db = PitcherPitchFXDatabaseConnection(sandbox_mode)
    else:
        db = BatterPitchFXDatabaseConnection(sandbox_mode)
    with ThreadPoolExecutor(os.cpu_count()) as executor1:
        for matchup, counts in pitch_type_dict.items():
            for count, pitch_types in counts.items():
                fields = ''
                values = ''
                for pitch, total in pitch_types.items():
                    fields += ', ' + pitch
                    values += ', ' + str(round(total/length[matchup][count], 3))
                executor1.submit(db.write('insert into ' + matchup + '_' + count + '_pitch_type (uid, playerid, year'
                                          + fields + ', p_uid) values (default, "' + player_id + '", ' + str(year)
                                          + values + ', ' + str(p_uid) + ');'))
    db.close()


def write_swing_take_by_pitch(player_id, p_uid, year, swing_take_dict, player_type, length, sandbox_mode):
    print(swing_take_dict)
    if player_type == 'pitching':
        db = PitcherPitchFXDatabaseConnection(sandbox_mode)
    else:
        db = BatterPitchFXDatabaseConnection(sandbox_mode)
    with ThreadPoolExecutor(os.cpu_count()) as executor2:
        for matchup, counts in swing_take_dict.items():
            for count, pitch_types in counts.items():
                fields = ''
                values = ''
                for pitch, total in pitch_types.items():
                    fields += ', ' + pitch
                    values += ', ' + str(round(swing_take_dict[matchup][count][pitch]/length[matchup][count][pitch], 3))
                print('insert into ' + matchup + '_' + count + '_swing_take (uid, playerid, year' + fields + ', p_uid)'
                      ' values (default, "' + player_id + '", ' + str(year) + values + ', ' + str(p_uid) + ');')
                executor2.submit(db.write('insert into ' + matchup + '_' + count + '_swing_take (uid, playerid, year'
                                          + fields + ', p_uid) values (default, "' + player_id + '", ' + str(year)
                                          + values + ', ' + str(p_uid) + ');'))
    db.close()


def write_ball_strike_by_pitch(player_id, p_uid, year, ball_strike_dict, player_type, length, sandbox_mode):
    print(ball_strike_dict)
    if player_type == 'pitching':
        db = PitcherPitchFXDatabaseConnection(sandbox_mode)
    else:
        db = BatterPitchFXDatabaseConnection(sandbox_mode)
    with ThreadPoolExecutor(os.cpu_count()) as executor3:
        for matchup, counts in ball_strike_dict.items():
            for count, pitch_types in counts.items():
                fields = ''
                values = ''
                for pitch, total in pitch_types.items():
                    fields += ', ' + pitch
                    values += ', ' + str(round(ball_strike_dict[matchup][count][pitch]/length[matchup][count][pitch], 3))
                print('insert into ' + matchup + '_' + count + '_ball_strike (uid, playerid, year' + fields + ', p_uid)'
                      ' values (default, "' + player_id + '", ' + str(year) + values + ', ' + str(p_uid) + ');')
                executor3.submit(db.write('insert into ' + matchup + '_' + count + '_ball_strike (uid, playerid, year'
                                          + fields + ', p_uid) values (default, "' + player_id + '", ' + str(year)
                                          + values + ', ' + str(p_uid) + ');'))
    db.close()


aggregate_pitch_fx_data(2009, Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\"
                                     "dump.log"), False)
