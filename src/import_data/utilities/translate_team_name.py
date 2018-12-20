def translate_team_name(abv):
    file = open("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\background\\team_finder.txt", 'rt')
    for line in file:
        tempLine = line.split(",")
        if abv == tempLine[0]:
            return tempLine[1]
