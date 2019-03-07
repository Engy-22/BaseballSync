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
    aggregate_and_write(year, 'batting')
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
        # break
    logger.log("\tDone aggregating and writing " + player_type + " data: Time = "
               + time_converter(time.time() - pitcher_time))


def aggregate(year, player_id, player_type):
    print(player_id)
    table = player_type[:-3] + 'er_pitches'
    matchups = ['vr', 'vl']
    opponent = 'hb' if player_type == 'pitching' else 'hp'
    pitch_usage = {}
    pitch_type_outcomes = {}
    pitch_type_swing_rate = {}
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
        aggregate_hbp(player_id, year, matchup, p_uid, player_type)
        pitch_usage[matchup] = {}
        pitch_type_outcomes[matchup] = {}
        pitch_type_swing_rate[matchup] = {}
        for ball in range(4):
            for strike in range(3):
                count = str(ball) + '-' + str(strike)
                pitch_usage[matchup][count] = {}
                pitch_type_outcomes[matchup][count] = {}
                pitch_type_swing_rate[matchup][count] = {}
                bulk_query = 'from ' + table + ' where playerid = "' + player_id + '" and year = ' + str(year) + ' and'\
                             ' matchup = "' + matchup + opponent + '" and count = "' + count + '"'
                for pitch_type in set(db.read('select pitch_type ' + bulk_query + 'and swing_take = "swing";')):
                    pitch_type_outcomes[matchup][count][pitch_type[0]] = {}
                    pitch_type_swing_rate[matchup][count][pitch_type[0]] = 0
                    temp_bulk_query = bulk_query + ' and pitch_type = "' + pitch_type[0] + '"'
                    pitch_usage[matchup][count][pitch_type[0]] = int(db.read('select count(*) ' + temp_bulk_query
                                                                             + ';')[0][0])
                    for outcome in db.read('select outcome ' + temp_bulk_query + ' and swing_take = "swing";'):
                        if outcome[0] in pitch_type_outcomes[matchup][count][pitch_type[0]]:
                            pitch_type_outcomes[matchup][count][pitch_type[0]][outcome[0]] += 1
                        else:
                            pitch_type_outcomes[matchup][count][pitch_type[0]][outcome[0]] = 1
                        pitch_type_swing_rate[matchup][count][pitch_type[0]] += 1
                write_pitch_usage(player_id, p_uid, year, matchup, count, pitch_usage[matchup][count], player_type)
                write_outcomes(player_id, year, matchup, count, pitch_type_outcomes[matchup][count], player_type)
                write_swing_rate(player_id, year, matchup, count, pitch_usage[matchup][count],
                                 pitch_type_swing_rate[matchup][count], player_type)
                # write_ball_strike_by_pitch(player_id, p_uid, year, strikes_by_pitch_dict, player_type,
                #                            pitches_length_swing_take_and_ball_strike)
                # write_trajectory_by_outcome()
                # write_field_by_outcome()
                # write_direction_by_outcome()
    db.close()


def write_pitch_usage(player_id, p_uid, year, matchup, count, pitch_type_dict, player_type):
    if player_type != 'pitching':
        return
    db = PitcherPitchFXDatabaseConnection(sandbox_mode)
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


def write_outcomes(player_id, year, matchup, count, outcomes_by_pitch_type, player_type):
    if player_type == 'pitching':
        db = PitcherPitchFXDatabaseConnection(sandbox_mode)
    else:
        db = BatterPitchFXDatabaseConnection(sandbox_mode)
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
                if len(db.read('select * from outcomes where playerid = "' + player_id + '" and year = ' + str(year)
                               + ' and matchup = "' + matchup + '" and count = "' + count + '" and outcome = "'
                               + outcome + '";')) == 0:
                    executor4.submit(db.write('insert into outcomes (uid, playerid, year, matchup, count, outcome, '
                                              + pitch_type + ') values (default, "' + player_id + '", ' + str(year)
                                              + ', "' + matchup + '", "' + count + '", "' + outcome + '", '
                                              + str(round(total/total_pitches[pitch_type], 3)) + ');'))
                else:
                    executor4.submit(db.write('update outcomes set ' + pitch_type + ' = '
                                              + str(round(total/total_pitches[pitch_type], 3)) + ' where playerid = "'
                                              + player_id + '" and year = ' + str(year) + ' and matchup = "' + matchup
                                              + '" and count = "' + count + '" and outcome = "' + outcome + '";'))
    db.close()


def write_swing_rate(player_id, year, matchup, count, pitch_usage, swing_rates, player_type):
    if player_type == 'pitching':
        db = PitcherPitchFXDatabaseConnection(sandbox_mode)
    else:
        db = BatterPitchFXDatabaseConnection(sandbox_mode)
    if len(db.read('select uid from strike_percent where playerid = "' + player_id + '" and year = ' + str(year)
                   + ' and matchup = "' + matchup + '" and count = "' + count + '"')) == 0:
        fields = ''
        values = ''
        for pitch_type, swings in swing_rates.items():
            fields += ', ' + pitch_type
            values += ', ' + str(round(swings/pitch_usage[pitch_type], 3))
        db.write('insert into swing_rate (uid, playerid, year, matchup, count' + fields + ') values (default, "'
                 + player_id + '", ' + str(year) + ', "' + matchup + '", "' + count + '"' + values + ');')
    else:
        sets = ''
        for pitch_type, swings in swing_rates.items():
            pitch_usage + ' = ' + str(round(swings/pitch_usage[pitch_type], 3)) + ', '
        db.write('update swing_rate set ' + sets[:-2] + ' where playerid = "' + player_id + '", and year = '
                 + str(year) + ' and matchup = "' + matchup + '", and count = "' + count + '";')
    db.close()


def write_ball_strike_by_pitch(player_id, p_uid, year, ball_strike_dict, player_type, length):
    if player_type != 'pitching':
        return
    db = PitcherPitchFXDatabaseConnection(sandbox_mode)
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


def aggregate_hbp(player_id, year, matchup, p_uid, player_type):
    if player_type != 'pitching':
        return
    hbps = {}
    pitches = {}
    new_db = PitcherPitchFXDatabaseConnection(sandbox_mode)
    if len(new_db.read('select hbp_id from hbp where playerid = "' + player_id + '" and matchup = "' + matchup
                       + '" and year = ' + str(year) + ';')) == 0:
        new_db.close()
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
        new_db.write('insert into hbp (hbp_id, playerid, year, matchup' + fields + ', p_uid) values (default, "'
                     + player_id + '", ' + str(year) + ', "' + matchup + '"' + values + ', ' + str(p_uid) + ');')
    new_db.close()


aggregate_pitch_fx_data(2018)
# aggregate(2018, 'triggan01', 'pitching')
