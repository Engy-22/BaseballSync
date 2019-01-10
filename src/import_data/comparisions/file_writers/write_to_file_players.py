from utilities.DB_Connect import DB_Connect


def write_to_file(year, comps, comp_type):
    db, cursor = DB_Connect.grab("baseballData")
    for player, comp in comps.items():
        if comp is not None:
            if len(DB_Connect.read(cursor, 'select comp_id from comparisons_' + comp_type + '_overall where playerid ='
                                           + ' "' + player + '" and year = ' + str(year) + ';')) == 0:
                comp_pull = 1.0 - float(DB_Connect.read(cursor, 'select player_' + comp_type + '.certainty from player_'
                                                          + comp_type + ', player_teams where player_' + comp_type
                                                          + '.pt_uniqueidentifier=player_teams.pt_uniqueidentifier and'
                                                          + ' player_teams.playerid = "' + player + '" and player_'
                                                          + comp_type + '.year = ' + str(year) + ';')[0][0])
                # comp_pull = 1.0 if certainty == -1 else 1.0 - certainty
                comp_stat_id = int(DB_Connect.read(cursor, 'select player_' + comp_type + '.p' + comp_type[0] + '_'
                                                           + 'uniqueidentifier from player_' + comp_type
                                                           + ', player_teams where player_' + comp_type
                                                           + '.pt_uniqueidentifier=player_teams.pt_uniqueidentifier and'
                                                           + ' player_teams.playerid = "' + player + '" and player_'
                                                           + comp_type + '.year = ' + str(year) + ';')[0][0])
                DB_Connect.write(db, cursor, 'insert into comparisons_' + comp_type + '_overall (comp_id, playerId, '
                                             + 'year, comp, comp_year, comp_pull, comp_stat_id) values (default, "'
                                             + player + '", ' + str(year) + ', "' + comp.split(';')[0] + '", '
                                             + comp.split(';')[1] + ', ' + str(comp_pull) + ', ' + str(comp_stat_id)
                                             + ');')
            else:
                comp_pull = 1.0 - float(DB_Connect.read(cursor, 'select player_' + comp_type + '.certainty from player_'
                                                          + comp_type + ', player_teams where player_' + comp_type
                                                          + '.pt_uniqueidentifier=player_teams.pt_uniqueidentifier and'
                                                          + ' player_teams.playerid = "' + player + '" and player_'
                                                          + comp_type + '.year = ' + str(year) + ';')[0][0])
                # comp_pull = 1.0 if certainty == -1 else 1.0 - certainty
                comp_stat_id = int(DB_Connect.read(cursor, 'select player_' + comp_type + '.p' + comp_type[0] + '_'
                                                           + 'uniqueidentifier from player_' + comp_type
                                                           + ', player_teams where player_' + comp_type
                                                           + '.pt_uniqueidentifier=player_teams.pt_uniqueidentifier and'
                                                           + ' player_teams.playerid = "' + player + '" and player_'
                                                           + comp_type + '.year = ' + str(year) + ';')[0][0])
                comp_id = int(DB_Connect.read(cursor, 'select comp_id from comparisons_' + comp_type + '_overall where'
                                                      + ' playerid = "' + player + '" and year = ' + str(year)
                                                      + ';')[0][0])
                DB_Connect.write(db, cursor, 'update comparisons_' + comp_type + '_overall set comp = "'
                                             + comp.split(';')[0] + '", comp_year = ' + comp.split(';')[1] + ', '
                                             + 'comp_pull = ' + str(comp_pull) + ', comp_stat_id = ' + str(comp_stat_id)
                                             + ' where ' + 'comp_id = ' + str(comp_id) + ';')
        else:
            if len(DB_Connect.read(cursor, 'select comp_id from comparisons_' + comp_type + '_overall where playerid ='
                                           + ' "' + player + '" and year = ' + str(year) + ';')) == 0:
                DB_Connect.write(db, cursor, 'insert into comparisons_' + comp_type + '_overall (comp_id, playerId, '
                                             + 'year) values (default, "' + player + '", ' + str(year) + ');')
    DB_Connect.close(db)
