import csv


def resolve_team_id(team_abv):
    if team_abv not in ['aas', 'nas']:
        with open('C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\background\\pitch_fx_team_finders.csv',
                  'r') as file:
            rows = csv.reader(file)
            for row in rows:
                if row[0] == team_abv:
                    return row[1]
    return None


# print(resolve_team_id('tba'))
