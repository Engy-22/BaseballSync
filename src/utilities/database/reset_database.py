import os
from utilities.database.wrappers.baseball_data_connection import DatabaseConnection
from utilities.database.wrappers.pitchers_pitch_fx_connection import PitcherPitchFXDatabaseConnection
from utilities.database.wrappers.batters_pitch_fx_connection import BatterPitchFXDatabaseConnection
from concurrent.futures import ThreadPoolExecutor
import tkinter
import time
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
    tkinter.Button(new_frame, text="Submit", command=lambda: driver(new_frame, vars, all_years.get(),
                                                                    begin_year.get(), end_year.get()),
                   bg="white", cursor='hand2', font=font).grid(column=0, padx=5, pady=5)


def grey_boxes(all_years, begin, end):
    if all_years.get():
        begin.config(state="disabled")
        end.config(state="disabled")
    else:
        begin.config(state="normal")
        end.config(state="normal")


def driver(previous_frame, vars, all_years, begin_year, end_year):
    start_time = time.time()
    previous_frame.withdraw()
    if all_years:
        do_reset(vars, 'ALL')
    else:
        years = range(begin_year, end_year)
        for year in years:
            do_reset(vars, year)
    print(time_converter(time.time() - start_time))
    exit()


def do_reset(vars, year):
    frame.withdraw()
    clear_logs()
    for env, dbs in vars.items():
        for db, bool in dbs.items():
            if bool.get():
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
    proceed = True
    db = DatabaseConnection(sandbox_mode)
    if year == 'ALL':
        with open("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\background\\table_definitions.txt",
                  'rt') as file:
            table_defs = file.readlines()
            if sandbox_mode:
                print("removing existing tables - sandbox")
            else:
                print("removing existing tables - production")
            with ThreadPoolExecutor(os.cpu_count()) as executor:
                for line in reversed(table_defs):
                    executor.submit(db.write("drop table " + line.split('create table ')[1].split(' (')[0] + ';'))
    else:
        proceed = False
        if db.read('select year from years where year = ' + str(year) + ';'):
            baseball_data_year(db, sandbox_mode, year)
        else:
            print(str(year) + ' is not found in the database.')
    if proceed:
        with open("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\background\\table_definitions.txt",
                  'rt') as file:
            table_defs = file.readlines()
            if sandbox_mode:
                print("creating new tables - sandbox")
            else:
                print("creating new tables - production")
            with ThreadPoolExecutor(os.cpu_count()) as executor2:
                for line in table_defs:
                    executor2.submit(db.write(line))
    db.close()


def pitchers_pitch_fx(sandbox_mode, year):
    db = PitcherPitchFXDatabaseConnection(sandbox_mode)
    if year == 'ALL':
        with open("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\background\\pitch_fx_tables", 'rt')\
                as file:
            table_defs = file.readlines()
            if sandbox_mode:
                print("removing existing pitcher pitch fx tables - sandbox")
            else:
                print("removing existing pitcher pitch fx tables - production")
            with ThreadPoolExecutor(os.cpu_count()) as executor:
                for line in reversed(table_defs):
                    executor.submit(db.write("drop table " + line.split('create table ')[1].split(' (')[0] + ';'))
            with ThreadPoolExecutor(os.cpu_count()) as executor2:
                for table in db.read('show tables;'):
                    executor2.submit(db.write('drop table ' + table[0] + ';'))
            with open("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\background\\pitch_fx_tables", 'rt')\
                    as file:
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
        with open("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\background\\pitch_fx_tables", 'rt')\
                as file:
            table_defs = file.readlines()
            if sandbox_mode:
                print("removing existing batter pitch fx tables - sandbox")
            else:
                print("removing existing batter pitch fx tables - production")
            with ThreadPoolExecutor(os.cpu_count()) as executor:
                for line in reversed(table_defs):
                    executor.submit(db.write("drop table " + line.split('create table ')[1].split(' (')[0] + ';'))
            with ThreadPoolExecutor(os.cpu_count()) as executor2:
                for table in db.read('show tables;'):
                    if 'hbp_' not in table[0]:
                        executor2.submit(db.write('drop table ' + table[0] + ';'))
            with open("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\background\\pitch_fx_tables", 'rt')\
                    as file:
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
        if table[0].split('_')[0] in ['vr', 'vl', 'hbp']:
            db.write('delete from ' + table[0] + ' where year = ' + str(year) + ';')
        else:
            if str(year) in table[0]:
                db.write('drop table ' + table[0] + ';')


def baseball_data_year(db, sandbox_mode, year):
    if sandbox_mode:
        print('deleting ' + str(year) + ' sandbox baseballData')
    else:
        print('deleting ' + str(year) + ' production baseballData')
    with open("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\background\\table_definitions.txt",
              'rt') as file:
        table_defs = file.readlines()
        with ThreadPoolExecutor(os.cpu_count()) as executor2:
            for line in reversed(table_defs):
                table_name = line.split('create table ')[1].split(' (')[0]
                if table_name in ['ballpark_years', 'pitcher_pitches', 'batter_pitches', 'player_batting',
                                  'player_pitching', 'player_fielding', 'years', 'manager_year', 'schedule',
                                  'comparisons_batting_overall', 'comparisons_pitching_overall']:  # appending rows based on year field
                    executor2.submit(db.write('delete from baseballData.' + table_name + ' where year = ' + str(year)
                                              + ';'))
                elif table_name in ['hitter_spots', 'player_positions', 'starting_pitchers', 'team_years',
                                    'comparisons_team_offense_overall', 'comparisons_team_defense_overall']:  # appending rows based on another field
                    for ty_uid in db.read('select ty_uniqueidentifier from team_years where year = ' + str(year) + ';'):
                        executor2.submit(db.write('delete from baseballData.' + table_name + ' where '
                                                  'ty_uniqueidentifier = ' + str(ty_uid[0]) + ';'))


root = tkinter.Tk()
root.title('DB Manager')
root.withdraw()
frame = tkinter.Toplevel(root)
font = ('Times', 12)
vars = {}
label = tkinter.Label(frame, text="Reset Database", font=('Times', 14, 'bold'))
label.grid(row=0, column=0, padx=10, pady=10, columnspan=4)
for num, env in {1: 'Production', 2: 'Sandbox'}.items():
    vars[env] = {}
    tkinter.Label(frame, text=env, font=('Times', 12, 'underline')).\
        grid(row=1, column=(num-1)*2, columnspan=2, padx=5, pady=5)
    for num2, db in {1: 'baseballData', 2: 'pitchers_pitch_fx', 3: 'batters_pitch_fx'}.items():
        vars[env][db] = tkinter.BooleanVar()
        vars[env][db].set(False)
        tkinter.Checkbutton(frame, text=db, font=font, variable=vars[env][db], cursor="hand2").\
            grid(row=num2+1, column=num, padx=10, pady=5, sticky='w')
tkinter.Button(frame, text="Next", command=lambda: select_years(vars, frame), font=font, bg='white', cursor='hand2')\
    .grid(columnspan=4, padx=5, pady=5)
root.mainloop()
