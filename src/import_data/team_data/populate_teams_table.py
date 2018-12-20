from utilities.DB_Connect import DB_Connect
from src.import_data.utilities.translate_team_name import translate_team_name


def populate_teams_table(year):
    print("populating teams table")
    with open('..\\..\\background\\yearTeams.txt', 'rt') as file:
        db, cursor = DB_Connect.grab("baseballData")
        for line in file:
            if str(year) in line:
                temp_line = line.split(',')[1:-1]
                for team in temp_line:
                    team_id = team.split(';')[0]
                    DB_Connect.write(db, cursor, 'insert into teams (teamId, teamName) values ("' + team_id + '", "'
                                                 + translate_team_name(team_id).replace("'", "\'") + '");')
                break
    DB_Connect.close(db)
