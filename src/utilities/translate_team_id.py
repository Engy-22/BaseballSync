def translate_team_id(team, year):
    with open('C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\background\\yearTeams.txt', 'rt') as file:
        for line in file:
            if str(year) in line:
                temp_line = line.split(',')
                for team_names in temp_line:
                    if team in team_names:
                        return team_names.split(';')[0]