from utilities.database.wrappers.baseball_data_connection import DatabaseConnection
from utilities.properties import sandbox_mode
from model.teams.lineup_creator.driver import create_lineup
from model.players.player import Player


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
        return team_info

    def retrieve_roster(self):
        roster = []
        for player_data in self.team_info.split('&')[:-1]:
            player_info = player_data.split('*')
            roster.append(Player(player_info[0].split('*')[0], self.team_id, self.year))
            roster[-1].set_year_positions(self.retrieve_player_positions(player_info))
            if any(pitcher_type in roster[-1].get_year_positions() for pitcher_type in ['RP', 'SP']):
                roster[-1].set_pitching_stats(self.retrieve_pitching_stats(
                    player_info[1].split('standard_pitching_stats@')[1]))
            #     if any(position in roster[-1].get_year_positions() for position in ['C', '1B', '2B', '3B', 'SS', 'LF',
            #                                                                         'CF', 'RF', 'DH']):
            #         roster[-1].set_batting_stats(self.retrieve_batting_stats(player_info[1].
            #                                                                  split('standard_batting_stats@')[1]))
            # else:
            #     roster[-1].set_batting_stats(self.retrieve_batting_stats(player_info[1].
            #                                                              split('standard_batting_stats@')[1]))
            batting_spots = {}
            match_ups = {0: 'vr', 1: 'vl'}
            for match_up_num, match_up in match_ups.items():
                try:
                    temp_batting_info = player_info[1].split('|')[0].split('#')[match_up_num+1]
                    if '+vl' in temp_batting_info:
                        batting_info = temp_batting_info.split('+vl')[0].split('|')[0].split(',')
                    elif '+vr' in temp_batting_info:
                        batting_info = temp_batting_info.split('+vr')[0].split('|')[0].split(',')
                    else:
                        batting_info = temp_batting_info.split('|')[0].split(',')
                    batting_spots[match_up] = {}
                    for ent in batting_info:
                        try:
                            batting_spots[match_up][int(ent.split(':')[0])] = int(ent.split(':')[1])
                        except ValueError:
                            batting_spots[match_up][int(ent.split(':')[0])] = 0
                    roster[-1].set_batting_spots(batting_spots)
                except IndexError:
                    continue  # this occurs only when a player never started a game and was therefore has no batting order data
        return roster

    def set_lineup(self, starting_pitcher, opposing_pitcher, use_dh):
        batting_order, positions = create_lineup(self.team_id, self.year, self.roster, starting_pitcher,
                                                 opposing_pitcher.get_throwing_handedness(), use_dh)
        self.batting_order = batting_order
        self.defensive_lineup = positions
        self.pitcher = starting_pitcher
        # if self.pitcher.get_pitching_stats() is None:
        #     self.pitcher.get_pitching_stats()
        for player in self.batting_order:
            print(player.get_full_name())
            # if player.get_batting_stats() is None:
            #     player.get_batting_stats()
        print('SP:', self.pitcher.get_full_name(), '\n\n')

    @staticmethod
    def retrieve_pitching_stats(pitching_stats):
        standard_pitching_stats = {}
        temp_standard_pitching_stats = pitching_stats.split('advanced_pitching_stats')[0]
        for standard_stat in temp_standard_pitching_stats.split(','):
            if '.' in standard_stat.split(':')[1]:
                standard_pitching_stats[standard_stat.split(':')[0]] = float(standard_stat.split(':')[1])  # values that are floats
            else:
                try:
                    standard_pitching_stats[standard_stat.split(':')[0]] = int(standard_stat.split(':')[1])  # values that are ints
                except ValueError:
                    standard_pitching_stats[standard_stat.split(':')[0]] = standard_stat.split(':')[1]  # values that are of None type
        advanced_pitching_stats = {}
        temp_advanced_pitching_stats = pitching_stats.split('advanced_pitching_stats')[1].split('|')[0]
        for advanced_table in temp_advanced_pitching_stats.split('^'):
            table_name = advanced_table.split('#')[0].replace('@', '')
            advanced_pitching_stats[table_name] = {}
            for match_up_data in advanced_table.split('#')[1].split('%'):
                match_up = match_up_data.split(':')[0]
                advanced_pitching_stats[table_name][match_up] = {}
                try:
                    if any(individual in table_name for individual in ['swing_rate', 'strike_percent', 'hbp_pitching']):
                        if table_name == 'hbp_pitching':
                            for pitch_data in match_up_data.split(':')[1].split('+'):
                                advanced_pitching_stats[table_name][match_up][pitch_data.split('=')[0]] = \
                                    float(pitch_data.split('=')[1])
                        else:
                            for count_data in match_up_data.split(':')[1].split('+'):
                                count = count_data.split('_')[0]
                                advanced_pitching_stats[table_name][match_up][count] = {}
                                for pitch_data in count_data.split('_')[1].split(','):
                                    advanced_pitching_stats[table_name][match_up][count][pitch_data.split('=')[0]] = \
                                        float(pitch_data.split('=')[1])
                    elif 'by_outcome' not in table_name:
                        for count_data in match_up_data.split(':')[1].split(';'):
                            count = count_data.split('_')[0]
                            advanced_pitching_stats[table_name][match_up][count] = {}
                            outcome_data_list = count_data.split('_')[1].split('+')
                            for outcome_data in outcome_data_list:
                                outcome = outcome_data.split('>')[0]
                                advanced_pitching_stats[table_name][match_up][count][outcome] = {}
                                pitches = outcome_data.split('>')[1].split(',')
                                for pitch_data in pitches:
                                    advanced_pitching_stats[table_name][match_up][count][outcome]\
                                        [pitch_data.split('=')[0]] = float(pitch_data.split('=')[1])
                    else:
                        for outcome_data_list in match_up_data.split(':')[1].split('+'):
                            outcome = outcome_data_list.split('>')[0]
                            advanced_pitching_stats[table_name][match_up][outcome] = {}
                            for outcome_data in outcome_data_list.split('>')[1].split(','):
                                advanced_pitching_stats[table_name][match_up][outcome][outcome_data.split('=')[0]] = \
                                    float(outcome_data.split('=')[1])
                except IndexError:
                    advanced_pitching_stats[table_name][match_up] = None
        print(advanced_pitching_stats)
        return {'standard': standard_pitching_stats, 'advanced': advanced_pitching_stats}

    @staticmethod
    def retrieve_batting_stats(batting_stats):
        return ''

    @staticmethod
    def retrieve_player_positions(player_info):
        if 'vr#' in player_info[1]:
            return player_info[1].split('|')[1].split(',')
        else:
            return player_info[1].split('|')[0].split(',')
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


team = Team("CLE", 2017)
