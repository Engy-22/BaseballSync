import os
import sys
from wrappers.baseball_data_connection import DatabaseConnection
from wrappers.pitchers_pitch_fx_connection import PitcherPitchFXDatabaseConnection
from wrappers.batters_pitch_fx_connection import BatterPitchFXDatabaseConnection
from concurrent.futures import ThreadPoolExecutor
import time
from utilities.time_converter import time_converter
from utilities.clear_logs import clear_logs

sys.path.insert(0, os.path.dirname(os.getcwd()))


def driver(variables, all_years, begin_year=None, end_year=None):
    start_time = time.time()
    if all_years:
        do_reset(variables, 'ALL')
    else:
        years = range(begin_year, end_year)
        for year in years:
            do_reset(variables, year)
    print(time_converter(time.time() - start_time))
    exit()


def do_reset(variables, year):
    clear_logs(os.path.join("..", "..", "..", "logs"))
    for env, dbs in variables.items():
        for db in dbs:
            if env == 'Production':
                if db == 'baseballData':
                    baseball_data(False, year)
                elif db == 'pitchers_pitch_fx':
                    pitchers_pitch_fx(False, year)
                else:
                    batters_pitch_fx(False, year)
            else:
                if db == 'baseballData':
                    baseball_data(True, year)
                elif db == 'pitchers_pitch_fx':
                    pitchers_pitch_fx(True, year)
                else:
                    batters_pitch_fx(True, year)


def baseball_data(sandbox_mode, year):
    db = DatabaseConnection(sandbox_mode)
    if year == 'ALL':
        if sandbox_mode:
            print("removing existing tables - sandbox")
            db.write('drop database baseballData_sandbox')
            db.write('create database baseballData_sandbox')
        else:
            print("removing existing tables - production")
            db.write('drop database baseballData')
            db.write('create database baseballData')
        db = DatabaseConnection(sandbox_mode)
        with open(os.path.join("..", "..", "..", "background", "table_definitions.txt"), 'rt') as file:
            table_defs = file.readlines()
            if sandbox_mode:
                print("creating new tables - sandbox")
            else:
                print("creating new tables - production")
            with ThreadPoolExecutor(os.cpu_count()) as executor2:
                for line in table_defs:
                    executor2.submit(db.write(line))
    else:
        if db.read('select year from years where year = ' + str(year) + ';'):
            baseball_data_year(db, sandbox_mode, year)
        else:
            print(str(year) + ' is not found in the database.')
    db.close()


def pitchers_pitch_fx(sandbox_mode, year):
    db = PitcherPitchFXDatabaseConnection(sandbox_mode)
    if year == 'ALL':
        if sandbox_mode:
            print("removing existing pitcher pitch_fx tables - sandbox")
            db.write('drop database pitchers_pitch_fx_sandbox')
            db.write('create database pitchers_pitch_fx_sandbox')
        else:
            print("removing existing pitcher pitch_fx tables - production")
            db.write('drop database pitchers_pitch_fx')
            db.write('create database pitchers_pitch_fx')
        db = PitcherPitchFXDatabaseConnection(sandbox_mode)
        with open(os.path.join("..", "..", "..", "background", "table_definitions.txt"), 'rt') as file:
            table_defs = file.readlines()
            if sandbox_mode:
                print("creating new pitcher pitch fx tables - sandbox")
            else:
                print("creating new pitcher pitch fx tables - production")
            with ThreadPoolExecutor(os.cpu_count()) as executor3:
                for line in table_defs:
                    executor3.submit(db.write(line))
    else:
        pitch_fx_year(db, year)
    db.close()


def batters_pitch_fx(sandbox_mode, year):
    db = BatterPitchFXDatabaseConnection(sandbox_mode)
    if year == 'ALL':
        if sandbox_mode:
            print("removing existing batter pitch fx tables - sandbox")
            db.write('drop database batters_pitch_fx_sandbox;')
            db.write('create database batters_pitch_fx_sandbox;')
        else:
            print("removing existing batter pitch fx tables - production")
            db.write('drop database batters_pitch_fx;')
            db.write('create database batters_pitch_fx;')
        db = BatterPitchFXDatabaseConnection(sandbox_mode)
        with open(os.path.join("..", "..", "..", "background", "pitch_fx_tables.txt"), 'rt') as file:
            table_defs = file.readlines()
            if sandbox_mode:
                print("creating new batter pitch fx tables - sandbox")
            else:
                print("creating new batter pitch fx tables - production")
            with ThreadPoolExecutor(os.cpu_count()) as executor3:
                for line in table_defs:
                    executor3.submit(db.write(line))
    else:
        pitch_fx_year(db, year)
    db.close()


def pitch_fx_year(db, year):
    print('resetting ' + str(year) + ' pitch fx')
    for table in db.read('show tables;'):
        db.write('delete from ' + table[0] + ' where year = ' + str(year) + ';')


def baseball_data_year(db, sandbox_mode, year):
    if sandbox_mode:
        extension = "_sandbox."
        print('deleting ' + str(year) + ' baseballData - sandbox')
    else:
        extension = "."
        print('deleting ' + str(year) + ' baseballData - production')
    with open(os.path.join("..", "..", "..", "background", "table_definitions.txt"), 'rt') as file:
        table_defs = file.readlines()
        with ThreadPoolExecutor(os.cpu_count()) as executor2:
            for line in reversed(table_defs):
                table_name = line.split('create table ')[1].split(' (')[0]
                if table_name in ['ballpark_years', 'pitcher_pitches', 'batter_pitches', 'player_batting',
                                  'player_pitching', 'player_fielding', 'years', 'manager_year', 'schedule',
                                  'comparisons_batting_overall', 'comparisons_pitching_overall']:  # appending rows based on year field
                    executor2.submit(db.write('delete from baseballData' + extension + table_name + ' where year = '
                                              + str(year) + ';'))
                elif table_name in ['hitter_spots', 'player_positions', 'starting_pitchers', 'team_years',
                                    'comparisons_team_offense_overall', 'comparisons_team_defense_overall']:  # appending rows based on another field
                    for ty_uid in db.read('select ty_uniqueidentifier from team_years where year = ' + str(year) + ';'):
                        executor2.submit(db.write('delete from baseballData' + extension + table_name + ' where '
                                                  'ty_uniqueidentifier = ' + str(ty_uid[0]) + ';'))


if __name__ == '__main__':
    databases = ['baseballData', 'pitchers_pitch_fx', 'batters_pitch_fx']
    variables = {'Production': [], 'Sandbox': []}
    for database in databases:
        db = input('Reset ' + database + ' DB? (y|n): ')
        if db.lower() == 'y':
            prod_sandbox = input("Reset Production, Sandbox or both? (p|s|b): ")
            if prod_sandbox.lower() == 'p':
                variables['Production'].append(database)
            elif prod_sandbox.lower() == 's':
                variables['Sandbox'].append(database)
            else:
                variables['Production'].append(database)
                variables['Sandbox'].append(database)
    all_years = input("Reset database(s) for all year? (y|n): ")
    if all_years.lower == 'n':
        begin_year = input("Begin (year): ")
        end_year = input("End (year): ")
        driver(variables, False, int(begin_year), int(end_year))
    else:
        driver(variables, True)
