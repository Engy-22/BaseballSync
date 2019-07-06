import os
from utilities.database.wrappers.baseball_data_connection import DatabaseConnection
from utilities.database.db_table_description import get_db_field_names


def consolidate_player_stats(ty_uid, player_type, year):
    player_stats = {}
    db = DatabaseConnection(sandbox_mode=True)
    if player_type != 'fielding':
        for p_uid in db.read('select p_uid from direction_' + player_type + ' group by p_uid;'):
            if player_is_on_this_team(ty_uid, p_uid, player_type, year):
                player_id = get_player_id(p_uid, player_type)
                player_stats[player_id] = {}
                player_stats[player_id]['standard_' + player_type + '_stats'] = \
                    consolidate_traditional_player_stats(db.read('select * from player_' + player_type + ' where p'
                                                                 + player_type[0] + '_uniqueidentifier = ' + str(p_uid[0])
                                                                 + ';')[0],
                                                         get_db_field_names(db.read('describe player_' + player_type
                                                                                    + ';')))
                try:
                    with open(os.path.join("..", "background", player_type + "_pitch_fx_tables.csv")) as tables_file:
                        tables = tables_file.readlines()
                except FileNotFoundError:
                    with open(os.path.join("..", "..", "..", "background", player_type + "_pitch_fx_tables.csv")) as\
                            tables_file:
                        tables = tables_file.readlines()
                player_stats[player_id]['advanced_' + player_type + '_stats'] = {}
                for table in tables:
                    player_stats[player_id]['advanced_' + player_type + '_stats'][table[:-1]] = \
                        consolidate_pitch_fx(db.read('select * from ' + table[:-1] + ' where p_uid = ' + str(p_uid[0])
                                                     + ';'), table[:-1], get_db_field_names(db.read('describe '
                                                                                                    + table[:-1] + ';')))
                break
    else:
        for player_role in ['batting', 'pitching']:
            for p_uid in db.read('select p_uid from direction_' + player_role + ' group by p_uid;'):
                if player_is_on_this_team(ty_uid, p_uid, player_type, year):
                    player_id = get_player_id(p_uid, player_role)
                    player_stats[player_id] = {}
                    player_stats[player_id]['standard_' + player_type + '_stats'] = \
                        consolidate_traditional_player_stats(db.read(player_fielding_uid_query(p_uid, player_role,
                                                                                               year))[0],
                                                             get_db_field_names(db.read('describe player_fielding;')))
                    break
    db.close()
    return stringify_player_stats(player_stats, player_type)


def consolidate_traditional_player_stats(table_data, table_fields):
    stats_reached = False
    stat_string = ''
    for field_num in range(len(table_fields)):
        if stats_reached:
            stat_string += table_fields[field_num] + ':' + str(table_data[field_num]) + ','
        else:
            if table_fields[field_num] == 'complete_year':
                stats_reached = True
    return stat_string[:-1]


def consolidate_pitch_fx(table_data, table_name, table_fields):
    if table_name in ['direction_batting', 'field_batting', 'outcomes_batting', 'trajectory_batting',
                      'direction_pitching', 'field_pitching', 'outcomes_pitching', 'trajectory_pitching']:
        return stringify_consolidated_pitch_fx(consolidate_pitch_fx_vertical(table_data, table_fields), 'vertical',
                                               table_name)
    elif table_name in ['direction_by_outcome_batting', 'field_by_outcome_batting', 'trajectory_by_outcome_batting',
                        'pitch_usage', 'direction_by_outcome_pitching', 'field_by_outcome_pitching',
                        'trajectory_by_outcome_pitching']:
        return stringify_consolidated_pitch_fx(consolidate_pitch_fx_horizontal(table_data, table_fields), 'horizontal',
                                               table_name)
    else:  # [swing_rate_batting, strike_percent_batting, swing_rate_pitching, strike_percent_pitching, hbp]
        return stringify_consolidated_pitch_fx(consolidate_pitch_fx_individual(table_data, table_fields, table_name),
                                               'individual', table_name)


def consolidate_pitch_fx_vertical(table_data, table_fields):
    stats = {'vr': {}, 'vl': {}}
    for record in table_data:
        if record[4] not in stats[record[3]]:
            stats[record[3]][record[4]] = {}
        if record[5] not in stats[record[3]][record[4]]:
            stats[record[3]][record[4]][record[5]] = {}
        for pitch in range(len(record[6:-1])):
            if record[pitch+6] is not None:
                stats[record[3]][record[4]][record[5]][table_fields[pitch+6]] = float(record[pitch+6])
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
    if table_name == 'hbp':
        for record in table_data:
            for field in range(len(record[4:-1])):
                if record[field+4] is not None:
                    stats[record[3]][table_fields[field+5]] = float(record[field+4])
    else:
        for record in table_data:
            stats[record[3]][record[4]] = {}
            for field in range(len(record[5:-1])):
                if record[field+5] is not None:
                    stats[record[3]][record[4]][table_fields[field+5]] = float(record[field+5])
    return stats


def stringify_consolidated_pitch_fx(consolidated_dictionary, consolidation_type, table_name):
    stringified_consolidated_dictionary = ''
    if consolidation_type == 'vertical':
        for match_up, count_data in consolidated_dictionary.items():
            stringified_consolidated_dictionary += match_up + ':'
            for count, sub_headers in count_data.items():
                stringified_consolidated_dictionary += count + '-'
                for sub_header, pitch_types in sub_headers.items():
                    stringified_consolidated_dictionary += sub_header + '>'
                    for pitch_type, number in pitch_types.items():
                        stringified_consolidated_dictionary += pitch_type + '=' + str(number) + ','
                    stringified_consolidated_dictionary = stringified_consolidated_dictionary[:-1] + '+'
    elif consolidation_type == 'horizontal':
        for match_up, sub_headers in consolidated_dictionary.items():
            stringified_consolidated_dictionary += match_up + ':'
            for sub_header, secondary_sub_headers in sub_headers.items():
                stringified_consolidated_dictionary += sub_header + '>'
                for entity_type, entity in secondary_sub_headers.items():
                    stringified_consolidated_dictionary += entity_type + '=' + str(entity) + ','
                stringified_consolidated_dictionary = stringified_consolidated_dictionary[:-1] + '+'
    else:
        if table_name == 'hbp':
            for match_up, pitch_types in consolidated_dictionary.items():
                stringified_consolidated_dictionary += match_up + ':'
                for pitch_type, number in pitch_types.items():
                    if pitch_type:
                        stringified_consolidated_dictionary += pitch_type + '=' + str(number) + ','
                    else:
                        stringified_consolidated_dictionary += 'None,'
                stringified_consolidated_dictionary = stringified_consolidated_dictionary[:-1] + '+'
        else:
            for match_up, count_data in consolidated_dictionary.items():
                stringified_consolidated_dictionary += match_up + ':'
                for count, pitch_types in count_data.items():
                    stringified_consolidated_dictionary += count + '-'
                    for pitch_type, number in pitch_types.items():
                        stringified_consolidated_dictionary += pitch_type + '=' + str(number) + ','
                    stringified_consolidated_dictionary = stringified_consolidated_dictionary[:-1] + '+'
    return stringified_consolidated_dictionary[:-1]


def stringify_player_stats(player_stats, player_type):
    stringified_player_stats = {}
    for player_id, info in player_stats.items():
        this_string = ''
        for stat_type, stats in info.items():
            this_string += stat_type + '@'
            if isinstance(stats, dict):
                for table_name, second_stats in stats.items():
                    this_string += table_name + '#' + second_stats + '^'
            else:
                this_string += stats
        if player_type != 'fielding':
            stringified_player_stats[player_id] = this_string[:-1]
        else:
            stringified_player_stats[player_id] = this_string
    return stringified_player_stats


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
    for team_id in db.read('select pt_uniqueidentifier from player_' + player_type + ' where p' + player_type[0]
                           + '_uniqueidentifier = ' + str(p_uid[0]) + ' and year = ' + str(year) + ';'):
        print(team_id[0], ' == ', db.read('select teamId from team_years where ty_uniqueidentifier = ' + str(ty_uid) + ';')[0][0])
        if team_id[0] == db.read('select teamId from team_years where ty_uniqueidentifier = ' + str(ty_uid) + ';')[0][0]:
            print('\t\t\tFound a player')
            player_on_team = True
            break
    db.close()
    return player_on_team


def player_fielding_uid_query(p_uid, player_type, year):
    return 'select * from player_fielding where pf_uniqueidentifier = (select pf_uniqueidentifier from ' \
           'player_fielding where pt_uniqueidentifier = (select pt_uniqueidentifier from player_' + player_type\
           + ' where p' + player_type[0] + '_uniqueidentifier = ' + str(p_uid[0]) + ') and year = ' + str(year) + ');'


# print(consolidate_player_stats('fielding'))
