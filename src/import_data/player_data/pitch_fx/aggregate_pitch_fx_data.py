import os
from utilities.database.wrappers.baseball_data_connection import DatabaseConnection
from utilities.database.wrappers.pitchers_pitch_fx_connection import PitcherPitchFXDatabaseConnection
from utilities.database.wrappers.batters_pitch_fx_connection import BatterPitchFXDatabaseConnection
from utilities.logger import Logger
import time
import datetime
from concurrent.futures import ThreadPoolExecutor
from utilities.time_converter import time_converter
from utilities.properties import sandbox_mode, import_driver_logger as driver_logger

logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\logs\\import_data\\"
                "aggregate_pitch_fx_data.log")


def aggregate_pitch_fx_data(year):
    print('Aggregating pitch fx data')
    driver_logger.log("\tAggregating pitch fx data")
    start_time = time.time()
    logger.log("Aggregating pitch fx data for " + str(year) + ' || Timestamp: ' + datetime.datetime.today().
               strftime('%Y-%m-%d %H:%M:%S'))
    aggregate_and_write(year, 'pitching')
    # aggregate_and_write(year, 'batting')
    total_time = time_converter(time.time() - start_time)
    logger.log("Done aggregating " + str(year) + " pitch fx data: Time = " + total_time + '\n\n')
    driver_logger.log("\t\tTime = " + total_time)


def aggregate_and_write(year, player_type):
    pitcher_time = time.time()
    logger.log("\tAggregating " + player_type + " data and writing to database")
    db = DatabaseConnection(sandbox_mode)
    players = set(db.read('select playerid from ' + player_type[:-3] + 'er_pitches;'))
    db.close()
    for player_id in players:
        aggregate(year, player_id[0], player_type)
        break
    logger.log("\tDone aggregating and writing " + player_type + " data: Time = "
               + time_converter(time.time() - pitcher_time))


def aggregate(year, player_id, player_type):
    print(player_id)
    table = player_type[:-3] + 'er_pitches'
    matchups = ['vr', 'vl']
    aggregate_hbp(player_id, year, matchups)
    opponent = 'hb' if player_type == 'pitching' else 'hp'
    balls = [ball for ball in range(4)]
    strikes = [strike for strike in range(3)]
    pitches_length_pitch_type = {}
    pitches_length_swing_take_and_ball_strike = {}
    pitch_types_dict = {}
    swings_by_pitch_dict = {}
    strikes_by_pitch_dict = {}
    outcomes_by_pitch_type = {}
    outcomes_by_pitch_type_length = {}
    db = DatabaseConnection(sandbox_mode)
    temp_p_uid = []
    for pt_uid in db.read('select pt_uniqueidentifier from player_teams where playerid = "' + player_id + '";'):
        try:
            temp_p_uid.append(db.read('select p' + player_type[0] + '_uniqueidentifier from player_' + player_type
                                      + ' where year = ' + str(year) + ' and pt_uniqueidentifier = ' + str(pt_uid[0])
                                      + ';')[0][0])
        except IndexError:
            pass
    # if len(temp_p_uid) > 1:
    #     p_uid = db.read('select p' + player_type[0] + '_uniqueidentifier from player_' + player_type + ' where year = '
    #                     + str(year) + ' and pt_uniqueidentifier = (select pt_uniqueidentifier from player_teams where '
    #                     'playerid = "' + player_id + '" and teamid = "TOT")' + ';')[0][0]
    # else:
    #     p_uid = temp_p_uid[0]
    for matchup in matchups:
        pitches_length_pitch_type[matchup] = {}
        pitches_length_swing_take_and_ball_strike[matchup] = {}
        pitch_types_dict[matchup] = {}
        swings_by_pitch_dict[matchup] = {}
        strikes_by_pitch_dict[matchup] = {}
        outcomes_by_pitch_type[matchup] = {}
        outcomes_by_pitch_type_length[matchup] = {}
        for ball in balls:
            for strike in strikes:
                pitch_types_dict[matchup][str(ball)+str(strike)] = {}
                swings_by_pitch_dict[matchup][str(ball)+str(strike)] = {}
                strikes_by_pitch_dict[matchup][str(ball)+str(strike)] = {}
                outcomes_by_pitch_type[matchup][str(ball)+str(strike)] = {}
                outcomes_by_pitch_type_length[matchup][str(ball)+str(strike)] = {}
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
                    strikes_by_pitch_dict[matchup][str(ball)+str(strike)][pitch_type[0]] = 0
                    outcomes_by_pitch_type[matchup][str(ball)+str(strike)][pitch_type[0]] = {}
                    outcomes_by_pitch_type_length[matchup][str(ball)+str(strike)][pitch_type[0]] = 0
                    temp_bulk_query = bulk_query + ' and pitch_type = "' + pitch_type[0] + '"'
                    for outcome in db.read('select outcome ' + temp_bulk_query + ' and swing_take = "swing";'):
                        if outcome[0] in outcomes_by_pitch_type[matchup][str(ball)+str(strike)][pitch_type[0]]:
                            outcomes_by_pitch_type[matchup][str(ball)+str(strike)][pitch_type[0]][outcome[0]] += 1
                        else:
                            outcomes_by_pitch_type[matchup][str(ball)+str(strike)][pitch_type[0]][outcome[0]] = 1
                        outcomes_by_pitch_type_length[matchup][str(ball) + str(strike)][pitch_type[0]] += 1
                    for swing_take in set(db.read('select swing_take ' + temp_bulk_query + ';')):
                        temp_bulk_query = bulk_query + ' and pitch_type = "' + pitch_type[0] + '" and swing_take = "'\
                                          + swing_take[0] + '" '#or outcome = "hbp")'
                        if swing_take[0] == 'swing':
                            swings_by_pitch_dict[matchup][str(ball)+str(strike)][pitch_type[0]] = \
                                    int(db.read('select count(*) ' + temp_bulk_query + ';')[0][0])
                        for ball_strike in set(db.read('select ball_strike ' + temp_bulk_query + ';')):
                            temp_bulk_query = bulk_query + ' and pitch_type = "' + pitch_type[0] + '" and swing_take='\
                                              + '"' + swing_take[0] + '" and ball_strike = "' + ball_strike[0] + '"'
                            if ball_strike[0] == 'strike':
                                strikes_by_pitch_dict[matchup][str(ball)+str(strike)][pitch_type[0]] += \
                                    int(db.read('select count(*) ' + temp_bulk_query)[0][0])
                            # for outcome in db.read('select outcome ' + temp_bulk_query + ';'):
                            #     print(outcome)
                            #     temp_bulk_query += ' and outcome = "' + outcome[0] + '"'
                            #     for trajectory in db.read('select trajectory ' + temp_bulk_query + ';'):
                            #         print(trajectory)
                            #         temp_bulk_query += ' and trajectory = "' + trajectory[0] + '"'
                            #         for field in db.read('select field ' + temp_bulk_query + ';'):
                            #             print(field)
                            #             temp_bulk_query += ' and field = "' + field[0] + '"'
                            #             count = 0
                            #             for direction in db.read('select direction ' + temp_bulk_query + ';'):
                            #                 print(direction)
                            #                 count += 1
    # write_pitch_usage(player_id, p_uid, year, pitch_types_dict, player_type, pitches_length_pitch_type)
    # write_swing_take_by_pitch(player_id, p_uid, year, swings_by_pitch_dict, player_type,
    #                           pitches_length_swing_take_and_ball_strike)
    # write_ball_strike_by_pitch(player_id, p_uid, year, strikes_by_pitch_dict, player_type,
    #                            pitches_length_swing_take_and_ball_strike)
    write_outcome_by_pitch_type(player_id, year, outcomes_by_pitch_type, outcomes_by_pitch_type_length, player_type)
    # write_trajectory_by_outcome()
    # write_field_by_outcome()
    # write_direction_by_outcome()
    db.close()


def write_pitch_usage(player_id, p_uid, year, pitch_type_dict, player_type, length):
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


def write_swing_take_by_pitch(player_id, p_uid, year, swing_take_dict, player_type, length):
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
                executor2.submit(db.write('insert into ' + matchup + '_' + count + '_swing_take (uid, playerid, year'
                                          + fields + ', p_uid) values (default, "' + player_id + '", ' + str(year)
                                          + values + ', ' + str(p_uid) + ');'))
    db.close()


def write_ball_strike_by_pitch(player_id, p_uid, year, ball_strike_dict, player_type, length):
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
                    values += ', ' + str(round(ball_strike_dict[matchup][count][pitch]/length[matchup][count][pitch],3))
                executor3.submit(db.write('insert into ' + matchup + '_' + count + '_ball_strike (uid, playerid, year'
                                          + fields + ', p_uid) values (default, "' + player_id + '", ' + str(year)
                                          + values + ', ' + str(p_uid) + ');'))
    db.close()


def write_outcome_by_pitch_type(player_id, year, outcomes_by_pitch_type, length, player_type):
    if player_type == 'pitching':
        db = PitcherPitchFXDatabaseConnection(sandbox_mode)
    else:
        db = BatterPitchFXDatabaseConnection(sandbox_mode)
    with ThreadPoolExecutor(os.cpu_count()) as executor4:
        for matchup, counts in outcomes_by_pitch_type.items():
            for count, pitch_types in counts.items():
                table = str(year) + '_' + player_id + '_' + matchup + '_' + count + '_outcomes'
                fields = ''
                records = {}
                for pitch_type, outcomes in pitch_types.items():
                    fields += ', ' + pitch_type + ' float'
                    for outcome, total in outcomes.items():
                        if outcome in records:
                            if pitch_type in records[outcome]:
                                records[outcome][pitch_type] += 1
                            else:
                                records[outcome][pitch_type] = 1
                        else:
                            records[outcome] = {}
                            records[outcome][pitch_type] = 1
                db.write('create table ' + table + ' (outcome text' + fields + ');')
                for outcome, pitch_types in records.items():
                    for pitch_type, total in pitch_types.items():
                        if outcome not in [temp[0] for temp in set(db.read('select outcome from ' + table + ';'))]:
                            executor4.submit(db.write('insert into ' + table + '(outcome, ' + pitch_type + ') values ("'
                                                      + outcome + '", '
                                                      + str(round(total/length[matchup][count][pitch_type], 3)) + ');'))
                        else:
                            executor4.submit(db.write('update ' + table + ' set ' + pitch_type + ' = '
                                                      + str(round(total/length[matchup][count][pitch_type], 3))
                                                      + ' where outcome = "' + outcome + '";'))
    db.close()


def aggregate_hbp(player_id, year, matchups):
    hbps = {}
    pitches = {}
    new_db = PitcherPitchFXDatabaseConnection(sandbox_mode)
    if len(new_db.read('select hbp_id from hbp_vrhb_pitch_type where playerid = "' + player_id + '" and year = "'
                       + str(year) + '";')) == 0:
        new_db.close()
        for matchup in matchups:
            db = DatabaseConnection(sandbox_mode)
            hbps[matchup] = db.read('select pitch_type, count(*) from pitcher_pitches where year = ' + str(year)
                                    + ' and playerid = "' + player_id + '" and matchup = "' + matchup + 'hb" and '
                                    'outcome = "hbp" group by pitch_type;')
            for pitch_type in db.read('select pitch_type, count(*) from pitcher_pitches where year = ' + str(year)
                                      + ' and playerid = "' + player_id + '" and matchup="vrhb" group by pitch_type;'):
                pitches[pitch_type[0]] = pitch_type[1]
            db.close()
        new_db = PitcherPitchFXDatabaseConnection(sandbox_mode)
        for matchup, hbp_by_pitch_type in hbps.items():
            fields = ''
            values = ''
            for hbp in hbp_by_pitch_type:
                fields += ', ' + hbp[0]
                values += ', ' + str(round(hbp[1]/pitches[hbp[0]], 3))
            new_db.write('insert into hbp_' + matchup + 'hb_pitch_type (hbp_id, playerid, year' + fields + ') values '
                         '(default, "' + player_id + '", ' + str(year) + values + ');')
        new_db.close()


# aggregate_pitch_fx_data(2009)
