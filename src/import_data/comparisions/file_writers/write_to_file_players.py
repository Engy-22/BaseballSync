from utilities.dbconnect import DatabaseConnection


def write_to_file(year, comps, comp_type):
    db = DatabaseConnection()
    for player, comp in comps.items():
        if comp is not None:
            comp_pull = 1.0 - float(db.read('select player_' + comp_type + '.certainty from player_' + comp_type
                                            + ', player_teams where player_' + comp_type + '.pt_uniqueidentifier = '
                                            'player_teams.pt_uniqueidentifier and player_teams.playerid = "' + player
                                            + '" and player_' + comp_type + '.year = ' + str(year) + ';')[0][0])
            if comp_pull == 0.0:
                continue
            comp_stat_id = int(db.read('select player_' + comp_type + '.p' + comp_type[0] + '_uniqueidentifier from'
                                       ' player_' + comp_type + ', player_teams where player_' + comp_type
                                       + '.pt_uniqueidentifier=player_teams.pt_uniqueidentifier and player_teams.'
                                       'playerid = "' + player + '" and player_' + comp_type + '.year = '
                                       + str(year) + ';')[0][0])
            if len(db.read('select comp_id from comparisons_' + comp_type + '_overall where playerid =' + ' "' + player
                           + '" and year = ' + str(year) + ';')) == 0:
                db.write('insert into comparisons_' + comp_type + '_overall (comp_id, playerId, year, comp, comp_year, '
                         'comp_pull, comp_stat_id) values (default, "' + player + '", ' + str(year) + ', "'
                         + comp.split(';')[0] + '", ' + comp.split(';')[1] + ', ' + str(comp_pull) + ', '
                         + str(comp_stat_id) + ');')
            else:
                comp_id = int(db.read('select comp_id from comparisons_' + comp_type + '_overall where playerid = "'
                                      + player + '" and year = ' + str(year) + ';')[0][0])
                db.write('update comparisons_' + comp_type + '_overall set comp = "' + comp.split(';')[0]
                         + '", comp_year = ' + comp.split(';')[1] + ', comp_pull = ' + str(comp_pull)
                         + ', comp_stat_id = ' + str(comp_stat_id) + ' where ' + 'comp_id = ' + str(comp_id) + ';')
    db.close()
