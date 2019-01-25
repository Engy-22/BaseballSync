import csv


def resolve_player_id(player_num):
    with open('C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\background\\master.csv', 'r') as csv_file:
        reader = csv.reader(csv_file)
        for row in reader:
            if str(player_num) == row[0]:
                return row[9]
