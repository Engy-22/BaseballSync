import os


def translate_team_name(abv):
    file = open(os.path.join("..", "background", "team_finder.txt"), 'rt')
    for line in file:
        temp_line = line.split(",")
        if abv == temp_line[0]:
            return temp_line[1]
