import os
import time
import tkinter
from utilities.database.wrappers.baseball_data_connection import DatabaseConnection
from utilities.database.wrappers.pitchers_pitch_fx_connection import PitcherPitchFXDatabaseConnection
from utilities.database.wrappers.batters_pitch_fx_connection import BatterPitchFXDatabaseConnection
from utilities.properties import sandbox_mode
from utilities.time_converter import time_converter
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


def submit(data):
    start_time = time.time()
    frame.withdraw()
    for data_type, entities in data.items():
        for boolean, years in entities.items():
            if boolean:
                migrate(data_type, years)
    print(time_converter(time.time() - start_time))
    exit()


def migrate(data_type, years):
    if "All" in years:
        migrate_all(data_type)
    else:
        for year in range(years[0], years[1]):
            migrate_year(data_type, year)


def migrate_all(data_type):
    print("Transferring all sandbox data to production environment")
    if data_type == 'baseball':
        db = DatabaseConnection(sandbox_mode)
        db_name = "baseballData"
    elif data_type == 'pitchers':
        db = PitcherPitchFXDatabaseConnection(sandbox_mode)
        db_name = "pitchers_pitch_fx"
    else:
        db = BatterPitchFXDatabaseConnection(sandbox_mode)
        db_name = "batters_pitch_fx"
    with open("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\background\\table_definitions.txt",
              'rt') as file:
        table_defs = file.readlines()
        with ThreadPoolExecutor(os.cpu_count()) as executor:
            for line in table_defs:
                table_name = line.split('create table ')[1].split(' (')[0]
                print('\t' + table_name)
                executor.submit(db.write('insert into ' + db_name + '.' + table_name + ' select * from ' + db_name
                                         + '_sandbox.' + table_name + ';'))
    db.close()


def migrate_year(data_type, year):
    print("Transferring " + str(year) + " sandbox data to production environment")
    if data_type == 'baseball':
        db = DatabaseConnection(sandbox_mode)
        db_name = "baseballData"
    elif data_type == 'pitchers':
        db = PitcherPitchFXDatabaseConnection(sandbox_mode)
        db_name = "pitchers_pitch_fx"
    else:
        db = BatterPitchFXDatabaseConnection(sandbox_mode)
        db_name = "batters_pitch_fx"
    with open("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\background\\table_definitions.txt",
              'rt') as file:
        table_defs = file.readlines()
        with ThreadPoolExecutor(os.cpu_count()) as executor:
            for line in table_defs:
                table_name = line.split('create table ')[1].split(' (')[0]
                print('\t' + table_name)
                executor.submit(db.write('insert into ' + db_name + '.' + table_name + ' select * from ' + db_name
                                         + '_sandbox.' + table_name + ';'))
    db.close()


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
                 {baseball_data_entry: baseball_data_string, baseball_data_entry_end: baseball_data_string_end}),
                    font=font, cursor="hand2"). \
    grid(row=1, column=0, padx=5, pady=5, sticky="W")
pitchers_pitch_fx_bool = tkinter.BooleanVar()
pitchers_pitch_fx_string = tkinter.StringVar()
tkinter.Label(frame, text="Begin Year", font=font).grid(row=2, column=2, padx=(10, 5), pady=5, sticky="W")
pitchers_pitch_fx_entry = tkinter.Entry(frame, textvariable=pitchers_pitch_fx_string, width=6)
pitchers_pitch_fx_entry.grid(row=2, column=3, padx=5, pady=5, sticky="W")
pitchers_pitch_fx_entry.config(state="disabled")
pitchers_pitch_fx_string_end = tkinter.StringVar()
tkinter.Label(frame, text="End Year", font=font).grid(row=2, column=4, padx=(10, 5), pady=5, sticky="W")
pitchers_pitch_fx_entry_end = tkinter.Entry(frame, textvariable=pitchers_pitch_fx_string_end, width=6)
pitchers_pitch_fx_entry_end.grid(row=2, column=5, padx=5, pady=5, sticky="W")
pitchers_pitch_fx_entry_end.config(state="disabled")
pitchers_pitch_fx_all_years = tkinter.BooleanVar()
pitchers_pitch_all_years_checkbutton = tkinter.Checkbutton(frame, text="All Years", font=font, command=lambda:
enable_disable_2(pitchers_pitch_fx_all_years, {pitchers_pitch_fx_entry: pitchers_pitch_fx_string,
                                               pitchers_pitch_fx_entry_end: pitchers_pitch_fx_string_end}),
                                                           variable=pitchers_pitch_fx_all_years)
pitchers_pitch_all_years_checkbutton.grid(row=2, column=1, sticky="W", padx=(10, 5), pady=5)
pitchers_pitch_all_years_checkbutton.config(state="disabled")
tkinter.Checkbutton(frame, text="pitchers_pitch_fx", variable=pitchers_pitch_fx_bool, command=lambda:
enable_disable_1(pitchers_pitch_fx_bool, pitchers_pitch_all_years_checkbutton, pitchers_pitch_fx_all_years,
                 {pitchers_pitch_fx_entry: pitchers_pitch_fx_string,
                  pitchers_pitch_fx_entry_end: pitchers_pitch_fx_string_end}), font=font, cursor="hand2"). \
    grid(row=2, column=0, padx=5, pady=5, sticky="W")
batters_pitch_fx_bool = tkinter.BooleanVar()
batters_pitch_fx_string = tkinter.StringVar()
tkinter.Label(frame, text="Begin Year", font=font).grid(row=3, column=2, padx=(10, 5), pady=5, sticky="W")
batters_pitch_fx_entry = tkinter.Entry(frame, textvariable=batters_pitch_fx_string, width=6)
batters_pitch_fx_entry.grid(row=3, column=3, padx=5, pady=5, sticky="W")
batters_pitch_fx_entry.config(state="disabled")
batters_pitch_fx_string_end = tkinter.StringVar()
tkinter.Label(frame, text="End Year", font=font).grid(row=3, column=4, padx=(10, 5), pady=5, sticky="W")
batters_pitch_fx_entry_end = tkinter.Entry(frame, textvariable=batters_pitch_fx_string_end, width=6)
batters_pitch_fx_entry_end.grid(row=3, column=5, padx=5, pady=5, sticky="W")
batters_pitch_fx_entry_end.config(state="disabled")
batters_pitch_fx_all_years = tkinter.BooleanVar()
batters_pitch_fx_all_years_checkbutton = tkinter.Checkbutton(frame, text="All Years", font=font, command=lambda:
enable_disable_2(batters_pitch_fx_all_years, {batters_pitch_fx_entry: batters_pitch_fx_string,
                                              batters_pitch_fx_entry_end: batters_pitch_fx_string_end}),
                                                             variable=batters_pitch_fx_all_years)
batters_pitch_fx_all_years_checkbutton.grid(row=3, column=1, sticky="W", padx=(10, 5), pady=5)
batters_pitch_fx_all_years_checkbutton.config(state="disabled")
tkinter.Checkbutton(frame, text="batters_pitch_fx", variable=batters_pitch_fx_bool, command=lambda:
enable_disable_1(batters_pitch_fx_bool, batters_pitch_fx_all_years_checkbutton, batters_pitch_fx_all_years,
                 {batters_pitch_fx_entry: batters_pitch_fx_string,
                  batters_pitch_fx_entry_end: batters_pitch_fx_string_end}
                 ), font=font, cursor="hand2"). \
    grid(row=3, column=0, padx=5, pady=5, sticky="W")
tkinter.Button(frame, text="Submit", command=lambda:
submit({'baseball': {baseball_data_bool.get(): [baseball_data_string.get(), baseball_data_string_end.get()]},
        'pitchers': {pitchers_pitch_fx_bool.get(): [pitchers_pitch_fx_string.get(), pitchers_pitch_fx_string_end.get()]},
        'batters': {batters_pitch_fx_bool.get(): [batters_pitch_fx_string.get(), batters_pitch_fx_string_end.get()]}}),
               font=font, bg="white", cursor="hand2"). \
    grid(columnspan=6, padx=5, pady=5)
root.withdraw()
root.mainloop()
