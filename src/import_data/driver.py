"""
    Run this with the year(s) you want to download data for.
    This populates all tables in the baseballData database.
"""

import os
import sys
import time
import datetime
import tkinter

sys.path.append(os.path.join(sys.path[0], '..'))
from utilities.stringify_list import stringify_list
from utilities.time_converter import time_converter
from utilities.get_most_recent_year import get_most_recent_year
from import_data.leagues.league_table_constructor import league_table_constructor
from import_data.team_data.manager_table_constructor import manager_table_constructor
from import_data.team_data.populate_teams_table import populate_teams_table
from import_data.leagues.year_data import get_year_data
from import_data.team_data.ballpark_and_manager_data import ballpark_and_manager_data
from import_data.leagues.league_standings import league_standings
from import_data.team_data.team_offensive_statistics import team_offensive_statistics
from import_data.team_data.team_defensive_statistics import team_defensive_statistics
from import_data.player_data.pitch_fx.pitch_fx import get_pitch_fx_data
from import_data.player_data.batting.batters import batting_constructor
from import_data.player_data.pitching.pitchers import pitching_constructor
from import_data.player_data.fielding.fielders import fielding_constructor
from import_data.team_data.team_fielding_file_constructor import team_fielding_file_constructor
from import_data.team_data.team_batting_order_constructor import team_batting_order_constructor
from import_data.team_data.team_pitching_rotation_constructor import team_pitching_rotation_constructor
from import_data.player_data.fielding.primary_and_secondary_positions import primary_and_secondary_positions
from import_data.player_data.batting.hitter_tendencies import hitter_tendencies
from import_data.player_data.pitching.pitcher_tendencies import pitcher_tendencies
from import_data.team_data.manager_tendencies import manager_tendencies
from import_data.player_data.batting.hitter_spray_chart_constructor import hitter_spray_chart_constructor
from import_data.player_data.pitching.pitching_spray_chart_constructor import pitcher_spray_chart_constructor
from import_data.team_data.team_certainties import team_certainties
from import_data.team_data.rank_driver import rank_driver
from import_data.player_data.awards.award_winner_driver import award_winner_driver
from import_data.comparisions.comparisons_driver import comparisons_driver
from import_data.player_data.awards.hof_finder import hof_finder
from utilities.clean_up_deadlocked_file import clean_up_deadlocked_file
from import_data.consolidata.driver import consolidate_data
from import_data.player_data.pitching.determine_pitcher_roles import determine_pitcher_roles_year
from utilities.properties import import_driver_logger as driver_logger


def main(from_server, begin_year, end_year, frame=None):
    if not from_server:
        frame.withdraw()
    league_table_constructor()
    manager_table_constructor()
    years = []
    for year in range(begin_year, end_year, 1):
        years.append(year)
        driver(year)
    rank_driver(years[-1])
    comparisons_driver(years[-1])
    hof_finder()
    clean_up_deadlocked_file()
    consolidate_data()
    driver_logger.log('Driver complete for year' + stringify_list(years) + ': time = '
                      + time_converter(time.time() - start_time) + '\n\n\n')
    exit()


def driver(year):
    driver_logger.log(str(year))
    driver_time = time.time()
    print('\n\n' + str(year))
    populate_teams_table(year)
    get_year_data(year)
    ballpark_and_manager_data(year)
    league_standings(year)
    team_offensive_statistics(year)
    team_defensive_statistics(year)
    batting_constructor(year)
    pitching_constructor(year)
    fielding_constructor(year)
    team_fielding_file_constructor(year)
    team_batting_order_constructor(year)
    team_pitching_rotation_constructor(year)
    primary_and_secondary_positions(year)
    determine_pitcher_roles_year(year)
    get_pitch_fx_data(year)
    hitter_tendencies(year)
    pitcher_tendencies(year)
    manager_tendencies(year)
    hitter_spray_chart_constructor(year)
    pitcher_spray_chart_constructor(year)
    team_certainties(year)
    award_winner_driver(year)
    driver_logger.log('Time taken to download ' + str(year) + ' data: ' + time_converter(time.time()-driver_time)
                      + '\n')


if __name__ == '__main__':
    driver_logger.log('Begin Driver || Timestamp: ' + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
    start_time = time.time()
    most_recent_year = get_most_recent_year()
    if 'win' in sys.platform:
        root = tkinter.Tk()
        root.title('Driver')
        root.withdraw()
        frame = tkinter.Toplevel(root)
        font = ('Times', 12)
        begin_year = tkinter.IntVar()
        end_year = tkinter.IntVar()
        tkinter.Label(frame, text="Begin Year", font=font).grid(row=0, column=0, padx=(20, 10), pady=10)
        tkinter.Label(frame, text="End Year", font=font).grid(row=1, column=0, padx=(20, 10), pady=10)
        tkinter.Entry(frame, textvariable=begin_year, width=7).grid(row=0, column=1, padx=(10, 20), pady=10)
        tkinter.Entry(frame, textvariable=end_year, width=7).grid(row=1, column=1, padx=(10, 20), pady=10)
        tkinter.Button(frame, text="Submit", command=lambda: main(False, begin_year.get(), end_year.get(), frame),
                       font=font, cursor="hand2", bg="white").grid(columnspan=2, padx=10, pady=10)
        root.mainloop()
    elif 'linux' in sys.platform:
        begin_year = int(input('Begin year: '))
        end_year = int(input('End year: '))
        main(True, begin_year, end_year)
    else:
        print('Unknown operating system. Must use Windows or Linux')
