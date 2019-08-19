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
        self.year_positions = []
        self.throws_with = None
        self.fielding_stats = None
        self.simulation_fielding_stats = {}
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

    def retrieve_throwing_handedness(self):
        db = DatabaseConnection(sandbox_mode)
        self.throws_with = db.read('select throwsWith from players where playerId = "' + self.player_id + '";')[0][0]
        db.close()

    def retrieve_full_name(self):
        db = DatabaseConnection(sandbox_mode)
        name = db.read('select firstName, lastName from players where playerId = "' + self.player_id + '";')[0]
        self.first_name = name[0]
        self.last_name = name[1]
        db.close()

    def construct_image_url(self):
        return os.path.join('images', 'players', self.player_id)
### End Retrievers ###

### Setters ###
    def set_year_positions(self, year_positions):
        self.year_positions = year_positions

    def set_fielding_stats(self, fielding_stats):
        self.fielding_stats = fielding_stats

    def set_simulation_fielding_stats(self, stat, value):
        self.simulation_fielding_stats[stat] = value
### End Setters ###

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

    def get_throwing_handedness(self):
        return self.throws_with

    def get_year_positions(self):
        return self.year_positions

    def get_fielding_stats(self):
        return self.fielding_stats

    def get_image_url(self):
        return self.image_url

    def get_full_name(self):
        try:
            return self.first_name + ' ' + self.last_name
        except TypeError:
            return None

    def get_simulation_fielding_stats(self, stat):
        return self.simulation_fielding_stats[stat]
### End Getters ###
