import os


def translate_team_id(team, year):
    try:
        file = open(os.path.join("..", "background", "yearTeams.txt"), 'rt')
    except FileNotFoundError:
        file = open(os.path.join("..", "..", "..", "background", "yearTeams.txt"), 'rt')
    for line in file:
        if str(year) in line:
            temp_line = line.split(',')
            for team_names in temp_line:
                if team in team_names:
                    file.close()
                    return team_names.split(';')[0]
