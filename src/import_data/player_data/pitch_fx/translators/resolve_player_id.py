from utilities.dbconnect import DatabaseConnection
from xml.dom import minidom


def resolve_player_id(player_num):
    players_file = minidom.parse('C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\src\\import_data\\'
                                 'player_data\\pitch_fx\\xml\\players.xml')
    for ent in players_file.getElementsByTagName('player'):
        if ent.getAttribute('id') == str(player_num):
            last_name = ent.getAttribute('last')
            first_name = ent.getAttribute('first')
            break
    db = DatabaseConnection()
    pid = db.read('select playerid from players where lastName = ' + last_name + ' and firstName = ' + first_name + ';')
    if len(pid) == 1:
        player_id = pid[0][0]
    else:
        player_id = None
    db.close()
    return player_id
