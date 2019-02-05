import os
from utilities.connections.baseball_data_connection import DatabaseConnection
from concurrent.futures import ThreadPoolExecutor


def write_to_file(year, comps, comp_type, sandbox_mode):
    db = DatabaseConnection(sandbox_mode)
    with ThreadPoolExecutor(os.cpu_count()) as executor:
        for player, comp in comps.items():
            if comp is not None:
                comp_pull = 1.0 - float(db.read('select player_' + comp_type + '.certainty from player_' + comp_type
                                                + ', player_teams where player_' + comp_type + '.pt_uniqueidentifier = '
                                                'player_teams.pt_uniqueidentifier and player_teams.playerid = "'
                                                + player + '" and player_' + comp_type + '.year = ' + str(year)
                                                + ';')[0][0])
                comp_stat_id = int(db.read('select player_' + comp_type + '.p' + comp_type[0] + '_uniqueidentifier from'
                                           ' player_' + comp_type + ', player_teams where player_' + comp_type
                                           + '.pt_uniqueidentifier=player_teams.pt_uniqueidentifier and player_teams.'
                                           'playerid = "' + player + '" and player_' + comp_type + '.year = '
                                           + str(year) + ';')[0][0])
                if len(db.read('select comp_id from comparisons_' + comp_type + '_overall where playerid =' + ' "'
                               + player + '" and year = ' + str(year) + ';')) == 0:
                    executor.submit(transact, 'insert into comparisons_' + comp_type + '_overall (comp_id, playerId, '
                                              'year, comp, comp_year, comp_pull, comp_stat_id) values (default, "'
                                              + player + '", ' + str(year) + ', "' + comp.split(';')[0] + '", '
                                              + comp.split(';')[1] + ', ' + str(comp_pull) + ', ' + str(comp_stat_id)
                                              + ');', sandbox_mode)
                else:
                    comp_id = int(db.read('select comp_id from comparisons_' + comp_type + '_overall where playerid = "'
                                          + player + '" and year = ' + str(year) + ';')[0][0])
                    executor.submit(transact, 'update comparisons_' + comp_type + '_overall set comp = "'
                                              + comp.split(';')[0] + '", comp_year = ' + comp.split(';')[1]
                                              + ', comp_pull = ' + str(comp_pull) + ', comp_stat_id = '
                                              + str(comp_stat_id) + ' where ' + 'comp_id = ' + str(comp_id) + ';',
                                              sandbox_mode)
    db.close()


def transact(statement, sandbox_mode):
    db = DatabaseConnection(sandbox_mode)
    db.write(statement)
    db.close()
