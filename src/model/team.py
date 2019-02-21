from utilities.connections.baseball_data_connection import DatabaseConnection
from model.properties import sandbox_mode
from utilities.num_to_word import num_to_word


class Team:

    def __init__(self, team_id: str, year: int):
        self.team_id = team_id
        self.year = year
        self.roster = self.retrieve_roster()
        self.batting_spots = self.retrieve_batting_spots()
        self.batting_order = []
        self.defensive_lineup = []
        self.year_off_rank = None
        self.year_def_rank = None
        self.year_ovr_rank = None
        self.ovr_off_rank = None
        self.ovr_def_rank = None
        self.ovr_ovr_rank = None
        self.image_url = ""

### RETRIEVERS ###
    def retrieve_roster(self):
        db = DatabaseConnection(sandbox_mode)
        players_positions = {}
        for player, positions in dict(db.read('select playerId, positions from player_positions where '
                                              'ty_uniqueidentifier = (select TY_uniqueidentifier from team_years where '
                                              'teamId = "' + self.team_id + '" and year = ' + str(self.year)
                                              + ');')).items():
            players_positions[player] = positions.split(',')
        db.close()
        return players_positions

    def retrieve_batting_spots(self):
        db = DatabaseConnection(sandbox_mode)
        batting_spots = {}
        query_fields = ''
        for num in range(9):
            query_fields += num_to_word(num+1) + ', '
        for data in db.read('select playerId, ' + query_fields[:-2] + ' from hitter_spots where ty_uniqueidentifier'
                            ' = (select ty_uniqueidentifier from team_years where teamid = "' + self.team_id + '" and'
                            ' year = ' + str(self.year) + ');'):
            spot = 1
            spots = {}
            for total in data[1:]:
                spots[spot] = total
                spot += 1
            batting_spots[data[0]] = spots

        db.close()
        return batting_spots
### END RETRIEVERS ###

### SETTERS ###
    def add_player_to_roster(self, player_id, positions):
        self.roster[player_id] = positions

    def add_player_to_batting_spots(self, player_id, spots):
        self.batting_spots[player_id] = spots
### SETTERS ###

### GETTERS ###
    def get_team_id(self):
        return self.team_id

    def get_year(self):
        return self.year

    def get_roster(self):
        return self.roster

    def get_batting_spots(self):
        return self.batting_spots

    def get_year_off_rank(self):
        return self.year_off_rank

    def get_year_def_rank(self):
        return self.year_def_rank

    def get_year_ovr_rank(self):
        return self.year_ovr_rank

    def get_ovr_off_rank(self):
        return self.ovr_off_rank

    def get_ovr_def_rank(self):
        return self.ovr_def_rank

    def get_ovr_ovr_rank(self):
        return self.ovr_ovr_rank

    def get_team_image(self):
        return self.image_url
### GETTERS ###


# team = Team("TEX", 2016)
# print(team.get_batting_spots())
