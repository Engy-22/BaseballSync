import csv


def resolve_team_id(team_abv, year):
    with open('C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\background\\pitch_fx_team_finders\\'
              'pitch_fx_team_finders-' + str(year) + '.csv', 'r') as file:
        rows = csv.reader(file)
        for row in rows:
            if row[0] == team_abv:
                return row[1]
    return None
