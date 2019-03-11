import os
from utilities.database.wrappers.baseball_data_connection import DatabaseConnection
from concurrent.futures import ThreadPoolExecutor
from utilities.properties import sandbox_mode


def write_to_file(comparisons, stat_type):
    db = DatabaseConnection(sandbox_mode)
    with ThreadPoolExecutor(os.cpu_count()) as executor:
        for ty_uid, comp in comparisons.items():
            if comp is not None:
                comp_pull = 1 - float(db.read('select certainty from team_years where ty_uniqueidentifier = '
                                              + str(ty_uid) + ';')[0][0])
                if len(db.read('select comp_id from comparisons_team_' + stat_type + '_overall where '
                               'ty_uniqueidentifier = ' + str(ty_uid) + ';')) == 0:
                    executor.submit(transact, 'insert into comparisons_team_' + stat_type + '_overall (comp_id, '
                                              'ty_uniqueidentifier, comp, comp_pull) values (default, ' + str(ty_uid)
                                              + ', ' + str(comp) + ', ' + str(comp_pull) + ');')
                else:
                    db.write('update comparisons_team_' + stat_type + '_overall set comp = ' + str(comp)
                             + ', comp_pull = ' + str(comp_pull) + ' where ty_uniqueidentifier = ' + str(ty_uid) + ';')
            else:
                if len(db.read('select comp_id from comparisons_team_' + stat_type + '_overall where '
                               'ty_uniqueidentifier = ' + str(ty_uid) + ';')) == 0:
                    executor.submit(transact, 'insert into comparisons_team_' + stat_type + '_overall (comp_id, ty_'
                                              'uniqueidentifier) values (default, ' + str(ty_uid) + ');')
    db.close()


def transact(statement):
    db = DatabaseConnection(sandbox_mode)
    db.write(statement)
    db.close()
