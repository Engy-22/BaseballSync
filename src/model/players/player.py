from utilities.database.wrappers.baseball_data_connection import DatabaseConnection
from utilities.properties import sandbox_mode


class Player:
    
    def __init__(self, player_id: str, team_id: str, year: int):
        self.player_id = player_id
        self.team_id = team_id
        self.year = year
        self.primary_position = self.retrieve_primary_position()
        self.secondary_positions = self.retrieve_secondary_positions()
        self.batting_spots = {}
        self.year_positions = []
        self.throws_with = self.throwing_handedness()
        self.bats_with = self.batting_handedness()
        self.batting_stats = self.retrieve_batting_stats()
        self.pitching_stats = self.retrieve_pitching_stats()
        self.fielding_stats = self.retrieve_fielding_stats()
        self.image_url = self.construct_image_url()

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

    def throwing_handedness(self):
        db = DatabaseConnection(sandbox_mode)
        throws_with = db.read('select throwsWith from players where playerId = "' + self.player_id + '";')[0][0]
        db.close()
        return throws_with

    def retrieve_batting_stats(self):
        return 'batting stats'

    def retrieve_pitching_stats(self):
        return 'pitching stats'

    def retrieve_fielding_stats(self):
        return 'fielding stats'

    def construct_image_url(self):
        return 'images\\players\\' + self.player_id
### END Retrievers ###

### SETTERS ###
    def set_batting_spots(self, batting_spots):
        self.batting_spots = batting_spots

    def set_year_positions(self, year_positions):
        self.year_positions = year_positions
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
### End Getters ###


# p = Player('lindofr01', 'CLE', 2016)
# print(p.get_batting_spots())
