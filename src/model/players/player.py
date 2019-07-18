import os
import csv
from utilities.database.wrappers.baseball_data_connection import DatabaseConnection
from utilities.properties import sandbox_mode
from concurrent.futures import ThreadPoolExecutor


class Player:
    
    def __init__(self, player_id: str, team_id: str, year: int):
        self.player_id = player_id
        self.first_name = None
        self.last_name = None
        self.team_id = team_id
        self.year = year
        self.primary_position = None
        self.secondary_positions = None
        self.batting_spots = {}
        self.year_positions = []
        self.throws_with = None
        self.bats_with = None
        self.batting_stats = None
        self.pitching_stats = None
        self.fielding_stats = None
        self.image_url = None

### Retrievers ###
    def retrieve_primary_position(self):
        db = DatabaseConnection(sandbox_mode)
        position = db.read('select primaryPosition from players where playerId = "' + self.player_id + '";')[0][0]
        db.close()
        return position

    def retrieve_secondary_positions(self):
        db = DatabaseConnection(sandbox_mode)
        position = db.read('select secondaryPositions from players where playerId = "' + self.player_id + '";')[0][0]
        db.close()
        return position

    def batting_handedness(self):
        db = DatabaseConnection(sandbox_mode)
        bats_with = db.read('select batsWith from players where playerId = "' + self.player_id + '";')[0][0]
        db.close()
        return bats_with

    # def retrieve_batting_stats(self):
    #     try:
    #         tables_file = open(os.path.join("..", "background", "batting_pitch_fx_tables.csv"))
    #     except FileNotFoundError:
    #         tables_file = open(os.path.join("..", "..", "background", "batting_pitch_fx_tables.csv"))
    #     tables = tables_file.readlines()
    #     tables_file.close()
    #     db = DatabaseConnection(sandbox_mode=False)
    #     # for table in tables:
    #     #     print(table, db.read('select * from ' + table + ' where playerid = "' + self.player_id + '" and year = '
    #     #                          + str(self.year) + ';'))
    #     db.close()
    #     self.batting_stats = 'batting stats'
    #
    # def retrieve_pitching_stats(self):
    #     try:
    #         tables_file = open(os.path.join("..", "background", "pitching_pitch_fx_tables.csv"))
    #     except FileNotFoundError:
    #         tables_file = open(os.path.join("..", "..", "background", "pitching_pitch_fx_tables.csv"))
    #     tables = tables_file.readlines()
    #     tables_file.close()
    #     db = DatabaseConnection(sandbox_mode=False)
    #     # for table in tables:
    #     #     print(table, db.read('select * from ' + table[:-1] + ' where playerid = "' + self.player_id
    #     #                          + '" and year = ' + str(self.year) + ';'))
    #     db.close()
    #     self.pitching_stats = 'pitching stats'
    #
    # def retrieve_fielding_stats(self):
    #     self.fielding_stats = 'fielding stats'

    def construct_image_url(self):
        return os.path.join('images', 'players', self.player_id)
### END Retrievers ###

### SETTERS ###
    def set_batting_spots(self, batting_spots):
        self.batting_spots = batting_spots

    def set_year_positions(self, year_positions):
        self.year_positions = year_positions

    def set_batting_stats(self, batting_stats):
        self.batting_stats = batting_stats

    def set_pitching_stats(self, pitching_stats):
        self.pitching_stats = pitching_stats

    def set_fielding_stats(self, fielding_stats):
        self.fielding_stats = fielding_stats

    def set_throwing_handedness(self):
        db = DatabaseConnection(sandbox_mode)
        self.throws_with = db.read('select throwsWith from players where playerId = "' + self.player_id + '";')[0][0]
        db.close()

    def set_full_name(self):
        db = DatabaseConnection(sandbox_mode)
        name = db.read('select firstName, lastName from players where playerId = "' + self.player_id + '";')[0]
        self.first_name = name[0]
        self.last_name = name[1]
        db.close()
### END SETTERS ###

### Getters ###
    def get_player_id(self):
        return self.player_id

    def get_year(self):
        return self.year

    def get_team_id(self):
        return self.team_id

    def get_primary_position(self):
        return self.primary_position

    def get_secondary_positions(self):
        return self.secondary_positions

    def get_batting_handedness(self):
        return self.bats_with

    def get_throwing_handedness(self):
        return self.throws_with

    def get_year_positions(self):
        return self.year_positions

    def get_batting_stats(self):
        return self.batting_stats

    def get_pitching_stats(self):
        return self.pitching_stats

    def get_fielding_stats(self):
        return self.fielding_stats

    def get_image_url(self):
        return self.image_url

    def get_batting_spots(self):
        return self.batting_spots

    def get_full_name(self):
        try:
            return self.first_name + ' ' + self.last_name
        except TypeError:
            return None
### End Getters ###
