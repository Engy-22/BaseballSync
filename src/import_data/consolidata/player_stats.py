import os
from utilities.database.wrappers.baseball_data_connection import DatabaseConnection
from utilities.database.db_table_description import get_db_field_names


def consolidate_player_stats(player_type):
    player_stats = {}
    db = DatabaseConnection(sandbox_mode=True)
    for p_uid in db.read('select p_uid from direction_batting group by p_uid;'):
        player_stats['standard-' + player_type + '-stats'] = \
            consolidate_traditional_player_stats(db.read('select * from player_' + player_type + ' where p'
                                                         + player_type[0] + '_uniqueidentifier = ' + str(p_uid[0])
                                                         + ';')[0], get_db_field_names(db.read('describe player_'
                                                                                               + player_type + ';')))
        if player_type != 'fielding':
            try:
                with open(os.path.join("..", "background", player_type + "_pitch_fx_tables.csv")) as tables_file:
                    tables = tables_file.readlines()
            except FileNotFoundError:
                with open(os.path.join("..", "..", "..", "background", player_type + "_pitch_fx_tables.csv")) as\
                        tables_file:
                    tables = tables_file.readlines()
            player_stats['advanced-' + player_type + '-stats'] = {}
            for table in tables:
                player_stats['advanced-' + player_type + '-stats'][table[:-1]] = \
                    consolidate_pitch_fx(db.read('select * from ' + table[:-1] + ' where p_uid = "' + str(p_uid[0])
                                                 + '";'), table[:-1], get_db_field_names(db.read('describe '
                                                                                                 + table[:-1] + ';')))
            print(player_stats['advanced-' + player_type + '-stats'])
        break
    db.close()


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
        return consolidate_pitch_fx_vertical(table_data, table_fields)
    elif table_name in ['direction_by_outcome_batting', 'field_by_outcome_batting', 'trajectory_by_outcome_batting',
                        'pitch_usage', 'direction_by_outcome_pitching', 'field_by_outcome_pitching',
                        'trajectory_by_outcome_pitching']:
        return consolidate_pitch_fx_horizontal(table_data, table_fields)
    else:  # [swing_rate_batting, strike_percent_batting, swing_rate_pitching, strike_percent_pitching, hbp]
        return consolidate_pitch_fx_individual(table_data, table_fields)


def consolidate_pitch_fx_vertical(table_data, table_fields):
    stats = {'vr': {}, 'vl': {}}
    for field_num in range(len(table_fields[6:-1])):
        for record in table_data:
            if record[4] not in stats[record[3]]:
                stats[record[3]][record[4]] = {}
            if record[5] not in stats[record[3]][record[4]]:
                stats[record[3]][record[4]][record[5]] = {}
            for pitch in range(len(record[6:-1])):
                if record[pitch+6]:
                    stats[record[3]][record[4]][record[5]][table_fields[pitch+6]] = record[pitch+6]
    return stats


def consolidate_pitch_fx_horizontal(table_data, table_fields):
    return ''


def consolidate_pitch_fx_individual(table_data, table_fields):
    return ''


consolidate_player_stats('batting')
