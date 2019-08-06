import os
from utilities.database.wrappers.baseball_data_connection import DatabaseConnection
from utilities.database.db_table_description import get_db_field_names


def consolidate_player_stats(ty_uid, player_type, year):
    player_stats = {}
    db = DatabaseConnection(sandbox_mode=True)
    if player_type != 'fielding':
        for p_uid in db.read('select p_uid from direction_' + player_type + ' group by p_uid;'):
            p_uids = [p_uid[0]]
            if player_is_on_this_team(ty_uid, p_uid, player_type, year):
                if player_was_on_more_than_one_team(p_uid, player_type, year):
                    p_uids.append(get_uid_of_player_for_this_team(ty_uid, p_uid, player_type, year))
                player_id = get_player_id(p_uid, player_type)
                player_stats[player_id] = {}
                player_stats[player_id]['standard_' + player_type + '_stats'] = {}
                for uid in p_uids:
                    player_stats[player_id]['standard_' + player_type + '_stats'][get_team_id(uid, player_type)] = \
                        consolidate_traditional_player_stats(
                            db.read('select * from player_' + player_type + ' where p' + player_type[0]
                                    + '_uniqueidentifier = ' + str(uid) + ';')[0], get_db_field_names(
                                db.read('describe player_' + player_type + ';')))
                try:
                    with open(os.path.join("..", "background", player_type + "_pitch_fx_tables.csv")) as tables_file:
                        tables = tables_file.readlines()
                except FileNotFoundError:
                    with open(os.path.join("..", "..", "..", "background", player_type + "_pitch_fx_tables.csv")) as \
                            tables_file:
                        tables = tables_file.readlines()
                player_stats[player_id]['advanced_' + player_type + '_stats'] = {}
                for table in tables:
                    player_stats[player_id]['advanced_' + player_type + '_stats'][table[:-1]] = \
                        consolidate_pitch_fx(db.read('select * from ' + table[:-1] + ' where p_uid = ' + str(p_uid[0])
                                                     + ';'), table[:-1],
                                             get_db_field_names(db.read('describe ' + table[:-1] + ';')))
    else:
        for player_role in ['batting', 'pitching']:
            for p_uid in db.read('select p_uid from direction_' + player_role + ' group by p_uid;'):
                p_uids = [p_uid[0]]
                try:
                    if player_is_on_this_team(ty_uid, db.read(player_fielding_uid_query(
                            p_uid[0], player_role, year, selector='pf_uniqueidentifier'))[0], player_type, year):
                        if player_was_on_more_than_one_team(p_uid, player_type, year):
                            p_uids.append(get_uid_of_player_for_this_team(ty_uid, p_uid, player_type, year))
                        player_id = get_player_id(p_uid, player_role)
                        for uid in p_uids:
                            player_stats[player_id] = consolidate_traditional_player_stats(
                                db.read(player_fielding_uid_query(uid, player_role, year))[0],
                                get_db_field_names(db.read('describe player_fielding;')))
                except IndexError:
                    continue
    db.close()
    return player_stats


def consolidate_traditional_player_stats(table_data, table_fields):
    stats_reached = False
    stat_dict = {}
    for field_num in range(len(table_fields)):
        if stats_reached:
            stat_dict[table_fields[field_num]] = table_data[field_num]
        else:
            if table_fields[field_num] == 'complete_year':
                stats_reached = True
    return stat_dict


def consolidate_pitch_fx(table_data, table_name, table_fields):
    if table_name in ['direction_batting', 'field_batting', 'outcomes_batting', 'trajectory_batting',
                      'direction_pitching', 'field_pitching', 'outcomes_pitching', 'trajectory_pitching']:
        return consolidate_pitch_fx_vertical(table_data, table_fields)
    elif table_name in ['direction_by_outcome_batting', 'field_by_outcome_batting', 'trajectory_by_outcome_batting',
                        'pitch_usage_batting', 'pitch_usage_pitching', 'direction_by_outcome_pitching',
                        'field_by_outcome_pitching', 'trajectory_by_outcome_pitching']:
        return consolidate_pitch_fx_horizontal(table_data, table_fields)
    else:  # [swing_rate_batting, strike_percent_batting, swing_rate_pitching, strike_percent_pitching, hbp_pitching, hbp_batting, pitch_count_pitching, pitch_count_batting]
        return consolidate_pitch_fx_individual(table_data, table_fields, table_name)


def consolidate_pitch_fx_vertical(table_data, table_fields):
    stats = {'vr': {}, 'vl': {}}
    for record in table_data:
        match_up = record[3]
        count = record[4]
        outcome = record[5]
        if count not in stats[match_up]:
            stats[match_up][count] = {}
        if outcome not in stats[match_up][count]:
            stats[match_up][count][outcome] = {}
        for pitch in range(len(record[6:-1])):
            pitch_type = pitch+6
            if record[pitch_type] is not None:
                stats[match_up][count][outcome][table_fields[pitch_type]] = float(record[pitch_type])
    return stats


def consolidate_pitch_fx_horizontal(table_data, table_fields):
    stats = {'vr': {}, 'vl': {}}
    for record in table_data:
        stats[record[3]][record[4]] = {}
        for field in range(len(record[5:-1])):
            if record[field+5] is not None:
                stats[record[3]][record[4]][table_fields[field+5]] = float(record[field+5])
    return stats


def consolidate_pitch_fx_individual(table_data, table_fields, table_name):
    stats = {'vr': {}, 'vl': {}}
    if 'hbp' in table_name:
        for record in table_data:
            for field in range(len(record[4:-1])):
                if record[field+4] is not None:
                    stats[record[3]][table_fields[field+5]] = float(record[field+4])
    elif 'overall_' in table_name:
        if 'swing_rate' not in table_name:
            stats = {}
            for field in range(len(table_data[0][3:-1])):
                if table_data[0][field+3] is not None:
                    stats[table_fields[field+3]] = float(table_data[0][field+3])
        else:
            stats = {'ball': {}, 'strike': {}}
            for record in table_data:
                for field in range(len(table_data[0][4:-1])):
                    if record[3] == 1:
                        if record[field+4] is not None:
                            stats['strike'][table_fields[field+4]] = float(record[field+4])
                    else:
                        if record[field+4] is not None:
                            stats['ball'][table_fields[field+4]] = float(record[field+4])
    elif 'swing_rate' in table_name:
        for record in table_data:
            if record[4] not in stats[record[3]]:
                stats[record[3]][record[4]] = {}
                stats[record[3]][record[4]]['strike'] = {}
                stats[record[3]][record[4]]['ball'] = {}
            for field in range(len(record[6:-1])):
                if record[field+6] is not None:
                    if record[5]:
                        stats[record[3]][record[4]]['strike'][table_fields[field+6]] = float(record[field+6])
                    else:
                        stats[record[3]][record[4]]['ball'][table_fields[field+6]] = float(record[field+6])
    else:
        for record in table_data:
            stats[record[3]][record[4]] = {}
            for field in range(len(record[5:-1])):
                if record[field+5] is not None:
                    if 'pitch_count' not in table_name:
                        stats[record[3]][record[4]][table_fields[field+5]] = float(record[field+5])
                    else:
                        stats[record[3]][record[4]][table_fields[field+5]] = int(record[field+5])
    return stats


def get_player_id(p_uid, player_type):
    db = DatabaseConnection(sandbox_mode=True)
    player_id = db.read('select playerId from player_teams where pt_uniqueidentifier = (select pt_uniqueidentifier from'
                        ' player_' + player_type + ' where p' + player_type[0] + '_uniqueidentifier = ' + str(p_uid[0])
                        + ');')[0][0]
    db.close()
    return player_id


def player_is_on_this_team(ty_uid, p_uid, player_type, year):
    player_on_team = False
    db = DatabaseConnection(sandbox_mode=True)
    this_players_uid_corresponding_team_id = \
        db.read('select teamId from player_teams where pt_uniqueidentifier = (select pt_uniqueidentifier from player_'
                + player_type + ' where p' + player_type[0] + '_uniqueidentifier = ' + str(p_uid[0]) + ' and year = '
                + str(year) + ');')[0][0]
    if this_players_uid_corresponding_team_id == db.read('select teamId from team_years where ty_uniqueidentifier = '
                                                         + str(ty_uid) + ';')[0][0]:
        player_on_team = True
    elif this_players_uid_corresponding_team_id == 'TOT':
        for pt_uid in db.read('select pt_uniqueidentifier from player_teams where playerId = (select playerId from '
                              'player_teams where pt_uniqueidentifier = (select pt_uniqueidentifier from player_'
                              + player_type + ' where year = ' + str(year) + ' and p' + player_type[0]
                              + '_uniqueidentifier = ' + str(p_uid[0]) + '));'):
            if db.read('select count(*) from player_' + player_type + ' where year = ' + str(year) + ' and '
                       'pt_uniqueidentifier = ' + str(pt_uid[0]) + ';')[0][0] > 0 and \
                    (db.read('select teamId from team_years where ty_uniqueidentifier = ' + str(ty_uid) + ';')[0][0] ==
                     db.read('select teamId from player_teams where pt_uniqueidentifier = ' + str(pt_uid[0])
                             + ';')[0][0]):
                player_on_team = True
    db.close()
    return player_on_team


def player_was_on_more_than_one_team(p_uid, player_type, year):
    db = DatabaseConnection(sandbox_mode=True)
    this_players_uid_corresponding_team_id = \
        db.read('select teamId from player_teams where pt_uniqueidentifier = (select pt_uniqueidentifier from player_'
                + player_type + ' where p' + player_type[0] + '_uniqueidentifier = ' + str(p_uid[0]) + ' and year = '
                + str(year) + ');')[0][0]
    db.close()
    if this_players_uid_corresponding_team_id == 'TOT':
        return True
    else:
        return False


def get_uid_of_player_for_this_team(ty_uid, p_uid, player_type, year):
    db = DatabaseConnection(sandbox_mode=True)
    uid = db.read('select p' + player_type[0] + '_uniqueidentifier from player_' + player_type + ' where year = '
                  + str(year) + ' and pt_uniqueidentifier = (select pt_uniqueidentifier from player_teams where '
                  'playerId = (select playerId from player_teams where pt_uniqueidentifier = (select '
                  'pt_uniqueidentifier from player_' + player_type + ' where p' + player_type[0] + '_uniqueidentifier '
                  '= ' + str(p_uid[0]) + ')) and teamId = (select teamId from team_years where ty_uniqueidentifier = '
                  + str(ty_uid) + '));')[0][0]
    db.close()
    return uid


def player_fielding_uid_query(p_uid, player_type, year, selector='*'):
    if selector == '*':
        return 'select * from player_fielding where pf_uniqueidentifier = (select pf_uniqueidentifier from ' \
               'player_fielding where pt_uniqueidentifier = (select pt_uniqueidentifier from player_' + player_type \
               + ' where p' + player_type[0] + '_uniqueidentifier = ' + str(p_uid) + ') and year = ' + str(year) + ');'
    else:
        return 'select pf_uniqueidentifier from player_fielding where pt_uniqueidentifier = (select ' \
               'pt_uniqueidentifier from player_' + player_type + ' where p' + player_type[0] + '_uniqueidentifier = ' \
               + str(p_uid) + ') and year = ' + str(year) + ';'


def get_team_id(uid, player_type):
    db = DatabaseConnection(sandbox_mode=True)
    team_id = db.read('select teamId from player_teams where pt_uniqueidentifier = (select pt_uniqueidentifier from '
                      'player_' + player_type + ' where p' + player_type[0] + '_uniqueidentifier = ' + str(uid)
                      + ')')[0][0]
    db.close()
    return team_id


# print(consolidate_player_stats(2, 'fielding', 2017))
