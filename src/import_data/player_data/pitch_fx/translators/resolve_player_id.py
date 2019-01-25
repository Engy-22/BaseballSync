from utilities.dbconnect import DatabaseConnection
from xml.dom import minidom
from import_data.player_data.pitch_fx.translators.resolve_team_id import resolve_team_id


def resolve_player_id(player_num, year, player_type):
    players_file = minidom.parse('C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\src\\import_data\\'
                                 'player_data\\pitch_fx\\xml\\players.xml')
    for ent in players_file.getElementsByTagName('player'):
        if ent.getAttribute('id') == str(player_num):
            last_name = ent.getAttribute('last')
            first_name = ent.getAttribute('first')
            team = ent.getAttribute('team_abbrev')
            break
    db = DatabaseConnection()
    pid = db.read('select playerid from players where lastName = ' + last_name + ' and firstName = ' + first_name + ';')
    if len(pid) == 1:
        player_id = pid[0][0]
    else:
        player_id = resolve_further(pid, team, year, player_type)
    db.close()
    if player_id is not None:
        return player_id
    else:
        return None


def resolve_further(pid, team, year, player_type):
    this_team = resolve_team_id(team, year)
    pt_uids = {}
    possible_match = []
    db = DatabaseConnection()
    for player_id in pid:
        pt_uids[player_id] = []
        for data in db.read('select pt_uniqueidentifier, teamid from player_teams where playerid = "' + player_id):
            pt_uids[player_id].append(data[0])
            pt_uids[player_id].append(data[1])
        for pt_uid in pt_uids[player_id]:
            if this_team == pt_uid[1]:  # if this is true, then we have a possible match
                for this_year in db.read('select year from player_' + player_type + ' where pt_uniqueidentifier = '
                                         + pt_uid[0]):
                    if this_year == str(year):
                        possible_match.append(player_id)
    db.close()
    if len(possible_match) == 1:
        return possible_match[0]
    else:
        return None
