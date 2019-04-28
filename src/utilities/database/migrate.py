import os
import sys
sys.path.append(os.path.join(sys.path[0], '..', '..'))

import tkinter
try:
    from wrappers.baseball_data_connection import DatabaseConnection
    from wrappers.pitchers_pitch_fx_connection import PitcherPitchFXDatabaseConnection
except ImportError:
    from utilities.database.wrappers.baseball_data_connection import DatabaseConnection
    from utilities.database.wrappers.pitch_fx_connection import PitchFXDatabaseConnection
from concurrent.futures import ThreadPoolExecutor

font = ('Times', 12)


def enable_disable_1(boolean, checkbutton, checkbutton_boolean, input_boxes):  # remove any input here
    if boolean.get():
        checkbutton.config(state="normal")
        for ent1, ent2 in input_boxes.items():
            ent1.config(state="normal")
    else:
        checkbutton.config(state="disabled")
        checkbutton_boolean.set(0)
        for ent1, ent2 in input_boxes.items():
            ent1.config(state="disabled")
            ent2.set("")


def enable_disable_2(boolean, input_boxes):
    if boolean.get():
        for ent1, ent2 in input_boxes.items():
            ent1.config(state="disabled")
            ent2.set("All")
    else:
        for ent1, ent2 in input_boxes.items():
            ent1.config(state="normal")
            ent2.set("")


def submit(data, from_server, auto):
    if not from_server:
        frame.withdraw()
    for db_name, entities in data.items():
        for boolean, years in entities.items():
            if boolean:
                migrate(db_name, years)
    if not auto:
        exit()


def migrate(db_name, years):
    if "All" in years:
        migrate_all(db_name)
    else:
        for year in range(int(years[0]), int(years[1])):
            migrate_year(db_name, year)


def migrate_all(db_name):
    print("Transferring all " + db_name + " sandbox data to production environment")
    if db_name == 'baseballData':
        from_db = DatabaseConnection(sandbox_mode=True)
        to_db = DatabaseConnection(sandbox_mode=False)
        to_db.write('drop database baseballData_sandbox;')
        to_db.write('create database baseballData_sandbox;')
        to_db.close()
        try:
            file = open(os.path.join("..", "..", "..", "background", "table_definitions.txt"), 'rt')
        except FileNotFoundError:
            file = open(os.path.join("..", "..", "baseball-sync", "background", "table_definitions.txt"), 'rt')
        table_defs = file.readlines()
        file.close()
        with ThreadPoolExecutor(os.cpu_count()) as executor:
            for line in table_defs:
                table_name = line.split('create table ')[1].split(' (')[0]
                executor.submit(from_db.write('insert into ' + db_name + '.' + table_name + ' select * from ' + db_name
                                              + '_sandbox.' + table_name + ';'))
    else:
        from_db = PitchFXDatabaseConnection(sandbox_mode=True)
        to_db = PitchFXDatabaseConnection(sandbox_mode=False)
        to_db.write('drop database baseballData;')
        to_db.write('create database baseballData;')
        to_db.close()
        try:
            file = open(os.path.join("..", "..", "..", "background", "pitch_fx_tables.txt"), 'rt')
        except FileNotFoundError:
            file = open(os.path.join("..", "..", "baseball-sync", "background", "pitch_fx_tables.txt"), 'rt')
        table_defs = [line.split('create table ')[1].split(' (')[0] for line in file.readlines()]
        file.close()
        with ThreadPoolExecutor(os.cpu_count()) as executor:
            for table in from_db.read('show tables;'):
                if table[0] not in table_defs:
                    fields = ''
                    for column in from_db.read('describe ' + table[0] + ';'):
                        fields += column[0] + ' ' + column[1] + ', '
                    from_db.write('create table ' + db_name + '.' + table[0] + ' (' + fields[:-2] + ');')
                executor.submit(from_db.write('insert into ' + db_name + '.' + table[0] + ' select * from ' + db_name
                                              + '_sandbox.' + table[0] + ';'))
    from_db.close()


def migrate_year(db_name, year):
    print("Transferring " + str(year) + " sandbox data to production environment")
    if db_name == 'baseballData':
        db = DatabaseConnection(sandbox_mode=True)
        try:
            file = open(os.path.join("..", "..", "..", "background", "table_definitions.txt"), 'rt')
        except FileNotFoundError:
            file = open(os.path.join("..", "..", "baseball-sync", "background", "table_definitions.txt"), 'rt')
        table_defs = file.readlines()
        file.close()
        with ThreadPoolExecutor(os.cpu_count()) as executor:
            for line in table_defs:
                table_name = line.split('create table ')[1].split(' (')[0]
                if table_name in ['ballpark_years', 'pitcher_pitches', 'batter_pitches', 'player_batting',
                                  'player_pitching', 'player_fielding', 'years', 'manager_year', 'schedule',
                                  'comparisons_batting_overall', 'comparisons_pitching_overall']:  # appending rows based on year field
                    executor.submit(db.write('insert into ' + db_name + '.' + table_name + ' select * from ' + db_name
                                             + '_sandbox.' + table_name + ' where year = ' + str(year) + ';'))
                elif table_name in ['hitter_spots', 'player_positions', 'starting_pitchers', 'team_years',
                                    'comparisons_team_offense_overall', 'comparisons_team_defense_overall']:  # appending rows based on another field
                    for ty_uid in db.read('select ty_uniqueidentifier from team_years where year = ' + str(year) + ';'):
                        executor.submit(db.write('insert into ' + db_name + '.' + table_name + ' select * from '
                                                 + db_name + '_sandbox.' + table_name + ' where ty_uniqueidentifier = '
                                                 + str(ty_uid[0]) + ';'))
                else:  # totally replacing the prod table with sandbox table ['leagues', 'managers', 'teams', 'ballparks', player_teams', 'manager_teams']
                    executor.submit(db.write('insert into ' + db_name + '.' + table_name + ' select * from ' + db_name
                                             + '_sandbox.' + table_name + ';'))
    else:
        db = PitchFXDatabaseConnection(sandbox_mode=True)
        try:
            file = open(os.path.join("..", "..", "..", "background", "table_definitions.txt"), 'rt')
        except FileNotFoundError:
            file = open(os.path.join("..", "..", "baseball-sync", "background", "table_definitions.txt"), 'rt')
        table_defs = file.readlines()
        file.close()
        with ThreadPoolExecutor(os.cpu_count()) as executor:
            for line in table_defs:
                table_name = line.split('create table ')[1].split(' (')[0]
                if table_name.split('_')[0] in ['vr', 'vl', 'hbp']:  # appending rows based on year field
                    executor.submit(db.write('insert into ' + db_name + '.' + table_name + ' select * from '
                                             + db_name + '_sandbox.' + table_name + ' where year = ' + str(year)
                                             + ';'))
                else:  # totally replacing the prod table with snadbox table ['']
                    if table_name.split(')')[0] == str(year):
                        executor.submit(db.write('insert into ' + db_name + '.' + table_name + ' select * from '
                                                 + db_name + '_sandbox.' + table_name + ' where year = '
                                                 + str(year) + ';'))
    db.close()


if __name__ == '__main__':
    if 'win' in sys.platform:
        root = tkinter.Tk()
        frame = tkinter.Toplevel(root)
        frame.title('Migrate')
        tkinter.Label(frame, text="Migrate Sandbox Data to Production", font=font).grid(columnspan=6, padx=10, pady=10)
        baseball_data_bool = tkinter.BooleanVar()
        baseball_data_string = tkinter.StringVar()
        tkinter.Label(frame, text="Begin Year", font=font).grid(row=1, column=2, padx=(10, 5), pady=5, sticky="W")
        baseball_data_entry = tkinter.Entry(frame, textvariable=baseball_data_string, width=6)
        baseball_data_entry.grid(row=1, column=3, padx=5, pady=5, sticky="W")
        baseball_data_entry.config(state="disabled")
        baseball_data_string_end = tkinter.StringVar()
        tkinter.Label(frame, text="End Year", font=font).grid(row=1, column=4, padx=(10, 5), pady=5, sticky="W")
        baseball_data_entry_end = tkinter.Entry(frame, textvariable=baseball_data_string_end, width=6)
        baseball_data_entry_end.grid(row=1, column=5, padx=5, pady=5, sticky="W")
        baseball_data_entry_end.config(state="disabled")
        baseball_data_all_years = tkinter.BooleanVar()
        baseball_data_all_years_checkbutton = tkinter.Checkbutton(frame, text="All Years", font=font, command=lambda:
            enable_disable_2(baseball_data_all_years, {baseball_data_entry: baseball_data_string,
                                                   baseball_data_entry_end: baseball_data_string_end}),
                                                                  variable=baseball_data_all_years)
        baseball_data_all_years_checkbutton.grid(row=1, column=1, sticky="W", padx=(10, 5), pady=5)
        baseball_data_all_years_checkbutton.config(state="disabled")
        tkinter.Checkbutton(frame, text="baseballData", variable=baseball_data_bool, command=lambda:
            enable_disable_1(baseball_data_bool, baseball_data_all_years_checkbutton, baseball_data_all_years,
                         {baseball_data_entry: baseball_data_string,
                          baseball_data_entry_end: baseball_data_string_end}), font=font, cursor="hand2"). \
            grid(row=1, column=0, padx=5, pady=5, sticky="W")
        pitch_fx_bool = tkinter.BooleanVar()
        pitch_fx_string = tkinter.StringVar()
        tkinter.Label(frame, text="Begin Year", font=font).grid(row=2, column=2, padx=(10, 5), pady=5, sticky="W")
        pitch_fx_entry = tkinter.Entry(frame, textvariable=pitch_fx_string, width=6)
        pitch_fx_entry.grid(row=2, column=3, padx=5, pady=5, sticky="W")
        pitch_fx_entry.config(state="disabled")
        pitch_fx_string_end = tkinter.StringVar()
        tkinter.Label(frame, text="End Year", font=font).grid(row=2, column=4, padx=(10, 5), pady=5, sticky="W")
        pitch_fx_entry_end = tkinter.Entry(frame, textvariable=pitch_fx_string_end, width=6)
        pitch_fx_entry_end.grid(row=2, column=5, padx=5, pady=5, sticky="W")
        pitch_fx_entry_end.config(state="disabled")
        pitch_fx_all_years = tkinter.BooleanVar()
        pitch_fx_all_years_checkbutton = tkinter.Checkbutton(frame, text="All Years", font=font, command=lambda:
        enable_disable_2(pitch_fx_all_years, {pitch_fx_entry: pitch_fx_string,
                                              pitch_fx_entry_end: pitch_fx_string_end}), variable=pitch_fx_all_years)
        pitch_fx_all_years_checkbutton.grid(row=2, column=1, sticky="W", padx=(10, 5), pady=5)
        pitch_fx_all_years_checkbutton.config(state="disabled")
        tkinter.Checkbutton(frame, text="pitch_fx", variable=pitch_fx_bool, command=lambda:
        enable_disable_1(pitch_fx_bool, pitch_fx_all_years_checkbutton, pitch_fx_all_years,
                         {pitch_fx_entry: pitch_fx_string,
                          pitch_fx_entry_end: pitch_fx_string_end}), font=font, cursor="hand2"). \
            grid(row=2, column=0, padx=5, pady=5, sticky="W")
        tkinter.Button(frame, text="Submit", command=lambda:
            submit({'baseballData': {baseball_data_bool.get(): [baseball_data_string.get(),
                                                                baseball_data_string_end.get()]},
                    'pitch_fx': {pitch_fx_bool.get(): [pitch_fx_string.get(),
                                                       pitch_fx_string_end.get()]}}, False, False),
                       font=font, bg="white", cursor="hand2").grid(columnspan=6, padx=5, pady=5)
        root.withdraw()
        root.mainloop()
    elif 'linux' in sys.platform:
        final_dict = {}
        databases = ['baseballData', 'pitch_fx']
        for database in databases:
            final_dict[database] = {}
            db = input('Migrate ' + database + ' DB? (y|n): ')
            if db.lower() == 'y':
                final_dict[database][True] = []
                all_years = input('All years? (y|n): ')
                if all_years.lower() == 'y':
                    final_dict[database][True] = ["All"]
                else:
                    final_dict[database][True].append(int(input('Begin year: ')))
                    final_dict[database][True].append(int(input('End year: ')))
            else:
                final_dict[database][False] = []
        submit(final_dict, True, False)
    else:
        print('Unknown operating system. Must use Windows or Linux')
