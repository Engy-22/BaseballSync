import os
import sys
sys.path.append(os.path.join(sys.path[0], '..', '..'))

import tkinter
import time
from wrappers.baseball_data_connection import DatabaseConnection
from wrappers.pitch_fx_connection import PitchFXDatabaseConnection
from concurrent.futures import ThreadPoolExecutor
from utilities.time_converter import time_converter
from utilities.clear_logs import clear_logs


def select_years(vars, previous_frame):
    previous_frame.withdraw()
    new_frame = tkinter.Toplevel(root)
    sub_frame = tkinter.Frame(new_frame)
    tkinter.Label(new_frame, text="Select a range of years to reset.", font=font).grid(padx=10, pady=10)
    tkinter.Label(sub_frame, text="Begin:", font=font).grid(row=1, column=0)
    tkinter.Label(sub_frame, text="End:", font=font).grid(row=1, column=2)
    begin_year = tkinter.IntVar()
    end_year = tkinter.IntVar()
    begin_entry = tkinter.Entry(sub_frame, text="Begin", textvariable=begin_year, width=7)
    begin_entry.grid(row=1, column=1, padx=(5, 10), pady=5)
    end_entry = tkinter.Entry(sub_frame, text="End", textvariable=end_year, width=7)
    end_entry.grid(row=1, column=3, padx=(5, 10), pady=5)
    all_years = tkinter.BooleanVar()
    tkinter.Checkbutton(sub_frame, text="All Years", command=lambda: grey_boxes(all_years, begin_entry, end_entry),
                        font=font, variable=all_years, cursor="hand2").grid(row=0, columnspan=4, padx=5, pady=5)
    sub_frame.grid(row=1, column=0)
    tkinter.Button(new_frame, text="Submit", command=lambda: driver(False, new_frame, vars, all_years.get(),
                                                                    begin_year.get(), end_year.get()),
                   bg="white", cursor='hand2', font=font).grid(column=0, padx=5, pady=5)


def grey_boxes(all_years, begin, end):
    if all_years.get():
        begin.config(state="disabled")
        end.config(state="disabled")
    else:
        begin.config(state="normal")
        end.config(state="normal")


def driver(from_server, previous_frame, vars, all_years, begin_year=None, end_year=None):
    if all_years or (end_year > begin_year >= 1876):
        start_time = time.time()
        if from_server:
            print('\n')
        else:
            previous_frame.withdraw()
        if all_years:
            do_reset(from_server, vars, 'ALL')
        else:
            years = range(begin_year, end_year)
            for year in years:
                do_reset(from_server, vars, year)
        print(time_converter(time.time() - start_time))
    else:
        print('Begin year must be lower than End year, but cannot be lower than 1876.')
    exit()


def do_reset(from_server, variables, year):
    clear_logs('import_data')
    if not from_server:
        frame.withdraw()
        for env, dbs in variables.items():
            for db, boolean in dbs.items():
                if boolean.get():
                    if env == 'Production':
                        if db == 'baseballData':
                            baseball_data(False, year)
                        else:
                            pitch_fx(False, year)
                    else:
                        if db == 'baseballData':
                            baseball_data(True, year)
                        else:
                            pitch_fx(True, year)
    else:
        for env, dbs in variables.items():
            for db in dbs:
                if env == 'Production':
                    if db == 'baseballData':
                        baseball_data(False, year)
                    else:
                        pitch_fx(False, year)
                else:
                    if db == 'baseballData':
                        baseball_data(True, year)
                    else:
                        pitch_fx(True, year)


def baseball_data(sandbox_mode, year):
    db = DatabaseConnection(sandbox_mode)
    if year == 'ALL':
        if sandbox_mode:
            print("removing existing baseballData tables - sandbox")
            db.write('drop database baseballData_sandbox')
            db.write('create database baseballData_sandbox')
        else:
            print("removing existing baseballData tables - production")
            db.write('drop database baseballData')
            db.write('create database baseballData')
        db = DatabaseConnection(sandbox_mode)
        with open(os.path.join("..", "..", "..", "background", "table_definitions.txt"), 'rt') as file:
            table_defs = file.readlines()
            if sandbox_mode:
                print("creating new baseballData tables - sandbox")
            else:
                print("creating new baseballData tables - production")
            with ThreadPoolExecutor(os.cpu_count()) as executor2:
                for line in table_defs:
                    executor2.submit(db.write(line))
    else:
        if db.read('select year from years where year = ' + str(year) + ';'):
            baseball_data_year(db, sandbox_mode, year)
        else:
            print(str(year) + ' is not found in the database.')
    db.close()


def pitch_fx(sandbox_mode, year):
    db = PitchFXDatabaseConnection(sandbox_mode)
    if year == 'ALL':
        if sandbox_mode:
            print("removing existing pitch_fx tables - sandbox")
            db.write('drop database pitch_fx_sandbox')
            db.write('create database pitch_fx_sandbox')
        else:
            print("removing existing pitch_fx tables - production")
            db.write('drop database pitch_fx')
            db.write('create database pitch_fx')
        db = PitchFXDatabaseConnection(sandbox_mode)
        with open(os.path.join("..", "..", "..", "background", "pitch_fx_tables.txt"), 'rt') as file:
            table_defs = file.readlines()
            if sandbox_mode:
                print("creating new pitch_fx tables - sandbox")
            else:
                print("creating new pitch_fx tables - production")
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
                        for ty_uid in db.read('select ty_uniqueidentifier from team_years where year = ' + str(year)
                                              + ';'):
                            executor2.submit(db.write('delete from baseballData' + extension + table_name + ' where '
                                                      'ty_uniqueidentifier = ' + str(ty_uid[0]) + ';'))


if __name__ == '__main__':
    if 'win' in sys.platform:
        root = tkinter.Tk()
        root.title('DB Manager')
        root.withdraw()
        frame = tkinter.Toplevel(root)
        font = ('Times', 12)
        variables = {}
        label = tkinter.Label(frame, text="Reset Database", font=('Times', 14, 'bold'))
        label.grid(row=0, column=0, padx=10, pady=10, columnspan=4)
        for num, env in {1: 'Production', 2: 'Sandbox'}.items():
            variables[env] = {}
            tkinter.Label(frame, text=env, font=('Times', 12, 'underline')).grid(row=1, column=(num - 1) * 2,
                                                                                 columnspan=2, padx=5, pady=5)
            for num2, db in {1: 'baseballData', 2: 'pitch_fx'}.items():
                variables[env][db] = tkinter.BooleanVar()
                variables[env][db].set(False)
                tkinter.Checkbutton(frame, text=db, font=font, variable=variables[env][db], cursor="hand2"). \
                    grid(row=num2 + 1, column=num, padx=10, pady=5, sticky='w')
        tkinter.Button(frame, text="Next", command=lambda: select_years(variables, frame), font=font, bg='white',
                       cursor='hand2').grid(columnspan=4, padx=5, pady=5)
        root.mainloop()
    elif 'linux' in sys.platform:
        databases = ['baseballData', 'pitch_fx']
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
        all_years = input("Reset database(s) for all years? (y|n): ")
        if all_years.lower() == 'n':
            begin_year = input("Begin (year): ")
            end_year = input("End (year): ")
            driver(True, None, variables, False, int(begin_year), int(end_year))
        else:
            driver(True, None, variables, True)
    else:
        print('Unknown operating system. Must use Windows or Linux')
