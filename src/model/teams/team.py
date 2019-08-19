from ast import literal_eval
from utilities.database.wrappers.baseball_data_connection import DatabaseConnection
from utilities.properties import sandbox_mode
from model.teams.lineup_creator.driver import create_lineup
from model.players.batter import Batter


class Team:

    def __init__(self, team_id: str, year: int):
        self.team_id = team_id
        self.year = year
        self.team_info = self.retrieve_team_info()
        self.roster = self.retrieve_roster()
        self.batting_order = []
        self.defensive_lineup = {}
        self.lineup_place = 0
        self.pitcher = ''
        self.year_off_rank = None
        self.year_def_rank = None
        self.year_ovr_rank = None
        self.ovr_off_rank = None
        self.ovr_def_rank = None
        self.ovr_ovr_rank = None
        self.image_url = ""

### RETRIEVERS ###
    def retrieve_team_info(self):
        db = DatabaseConnection(sandbox_mode)
        team_info = db.read('select team_info from team_years where teamId = "' + self.team_id + '" and year = '
                            + str(self.year) + ';')[0][0]
        db.close()
        return literal_eval(team_info)

    def retrieve_roster(self):
        # print(self.team_info['fielder_stats'])
        roster = []
        for player_id, positions in self.team_info['player_positions'].items():
            roster.append(Batter(player_id, self.team_id, self.year))
        return roster

    def set_lineup(self, starting_pitcher, opposing_pitcher, use_dh):
        batting_order, positions = create_lineup(self.team_id, self.year, self.roster, self.team_info, starting_pitcher,
                                                 opposing_pitcher, use_dh)
        self.batting_order = batting_order
        self.defensive_lineup = positions
        self.pitcher = starting_pitcher
        for player in self.batting_order:
            if not player.get_full_name():
                player.retrieve_full_name()
            print(player.get_full_name())
        if not self.pitcher.get_full_name():
            self.pitcher.retrieve_full_name()
        print('SP:', self.pitcher.get_full_name(), '\n\n')
### END RETRIEVERS ###

### SETTERS ###
    def add_player_to_roster(self, player_id, positions):
        self.roster[player_id] = positions

    def set_batting_order(self, batting_order):
        self.batting_order = batting_order

    def set_lineup_place(self, place):
        self.lineup_place = place

    def set_pitcher(self, pitcher):
        self.pitcher = pitcher
### SETTERS ###

### GETTERS ###
    def get_team_id(self):
        return self.team_id

    def get_team_info(self):
        return self.team_info

    def get_year(self):
        return self.year

    def get_roster(self):
        return self.roster

    def get_pitcher(self):
        return self.pitcher

    def get_batting_order(self):
        return self.batting_order

    def get_lineup_place(self):
        return self.lineup_place

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


# import time
# from utilities.time_converter import time_converter
# start_time = time.time()
# team = Team("CLE", 2017)
# print(time_converter(time.time() - start_time))
