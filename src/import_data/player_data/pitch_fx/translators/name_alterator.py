import csv


def name_alterator(first_name, last_name):
    with open('C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\src\\import_data\\player_data\\pitch_fx\\'
              'translators\\pitch_fx_player_name_alterations.csv', 'r') as file:
        rows = csv.reader(file)
        for row in rows:
            if first_name + ';' + last_name == row[0]:
                return row[1]
    return None
