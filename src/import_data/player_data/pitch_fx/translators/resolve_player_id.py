from utilities.dbconnect import DatabaseConnection
from xml.dom import minidom
from import_data.player_data.pitch_fx.translators.resolve_team_id import resolve_team_id


def resolve_player_id(player_num, year):
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
        player_id = resolve_further(pid, team, year)
    db.close()
    return player_id


def resolve_further(pid, team, year):
    this_team = resolve_team_id(team, year)
    pt_uids = {}
    db = DatabaseConnection()
    for player_id in pid:
        pt_uids[player_id] = db.read('select teamid from player_teams where playerid = "' + player_id)[0][0]
    db.close()
    return None
