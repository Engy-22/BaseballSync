import csv


def name_alterator(first_name):
    with open('C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\background\\'
              'pitch_fx_player_name_alterations.csv', 'r') as file:
        rows = csv.reader(file)
        for row in rows:
            if first_name == row[0]:
                return row[1]
    return None
