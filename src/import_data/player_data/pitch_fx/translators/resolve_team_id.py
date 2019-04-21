import os
import csv


def resolve_team_id(team_abv):
    if team_abv not in ['aas', 'nas']:
        with open(os.path.join("..", "..", "baseball-sync", "src", "import_data", "player_data", "pitch_fx",
                               "translators", "pitch_fx_team_finders.csv"), 'r') as file:
            rows = csv.reader(file)
            for row in rows:
                if row[0] == team_abv:
                    return row[1]
    return None
