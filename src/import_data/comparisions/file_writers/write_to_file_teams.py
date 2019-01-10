from utilities.DB_Connect import DB_Connect


def write_to_file(comparisons, stat_type):
    db, cursor = DB_Connect.grab("baseballData")
    for ty_uid, comp in comparisons.items():
        if comp is not None:
            comp_pull = 1 - float(DB_Connect.read(cursor, 'select certainty from team_years where ty_uniqueidentifier '
                                                          + '= ' + str(ty_uid) + ';')[0][0])
            if len(DB_Connect.read(cursor, 'select comp_id from comparisons_team_' + stat_type + '_overall where '
                                           + 'ty_uniqueidentifier = ' + str(ty_uid) + ';')) == 0:
                DB_Connect.write(db, cursor, 'insert into comparisons_team_' + stat_type + '_overall (comp_id, '
                                             'ty_uniqueidentifier, comp, comp_pull) values (default, ' + str(ty_uid)
                                             + ', ' + str(comp) + ', ' + str(comp_pull) + ');')
            else:
                DB_Connect.write(db, cursor, 'update comparisons_team_' + stat_type + '_overall set comp = ' + str(comp)
                                             + ', comp_pull = ' + str(comp_pull) + ' where ty_uniqueidentifier = '
                                             + str(ty_uid) + ';')
        else:
            if len(DB_Connect.read(cursor, 'select comp_id from comparisons_team_' + stat_type + '_overall where '
                                           + 'ty_uniqueidentifier = ' + str(ty_uid) + ';')) == 0:
                DB_Connect.write(db, cursor, 'insert into comparisons_team_' + stat_type + '_overall (comp_id, '
                                             + 'ty_uniqueidentifier) values (default, ' + str(ty_uid) + ');')
    DB_Connect.close(db)
