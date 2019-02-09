import os
from utilities.connections.baseball_data_connection import DatabaseConnection
from utilities.connections.pitchers_pitch_fx_connection import PitcherPitchFXDatabaseConnection
from utilities.connections.batters_pitch_fx_connection import BatterPitchFXDatabaseConnection
from concurrent.futures import ThreadPoolExecutor
import tkinter
import time
from utilities.time_converter import time_converter


def driver(vars):
    start_time = time.time()
    frame.withdraw()
    for env, dbs in vars.items():
        for db, bool in dbs.items():
            if bool.get():
                if env == 'Production':
                    if db == 'baseballData':
                        baseball_data(False)
                    elif db == 'pitchers_pitch_fx':
                        pitchers_pitch_fx(False)
                    else:
                        batters_pitch_fx(False)
                else:
                    if db == 'baseballData':
                        baseball_data(True)
                    elif db == 'pitchers_pitch_fx':
                        pitchers_pitch_fx(True)
                    else:
                        batters_pitch_fx(True)
    print(time_converter(time.time() - start_time))
    exit()


def baseball_data(sandbox_mode):
    db = DatabaseConnection(sandbox_mode)
    with open("..\\..\\background\\table_definitions.txt", 'rt') as file:
        table_defs = file.readlines()
        if sandbox_mode:
            print("removing existing tables - sandbox")
        else:
            print("removing existing tables - production")
        with ThreadPoolExecutor(os.cpu_count()) as executor:
            for line in reversed(table_defs):
                executor.submit(db.write("drop table " + line.split('create table ')[1].split(' (')[0] + ';'))
        if sandbox_mode:
            print("creating new tables - sandbox")
        else:
            print("creating new tables - production")
        with ThreadPoolExecutor(os.cpu_count()) as executor2:
            for line in table_defs:
                executor2.submit(db.write(line))
    db.close()

def pitchers_pitch_fx(sandbox_mode):
    db = PitcherPitchFXDatabaseConnection(sandbox_mode)
    with open("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\background\\pitch_fx_tables", 'rt') as file:
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
        if sandbox_mode:
            print("creating new pitcher pitch fx tables - sandbox")
        else:
            print("creating new pitcher pitch fx tables - production")
        with ThreadPoolExecutor(os.cpu_count()) as executor3:
            for line in table_defs:
                executor3.submit(db.write(line))
    db.close()


def batters_pitch_fx(sandbox_mode):
    db = BatterPitchFXDatabaseConnection(sandbox_mode)
    with open("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\background\\pitch_fx_tables", 'rt') as file:
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
        if sandbox_mode:
            print("creating new batter pitch fx tables - sandbox")
        else:
            print("creating new batther pitch fx tables - production")
        with ThreadPoolExecutor(os.cpu_count()) as executor3:
            for line in table_defs:
                executor3.submit(db.write(line))
    db.close()


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
        tkinter.Checkbutton(frame, text=db, font=font, variable=vars[env][db]).\
            grid(row=num2+1, column=num, padx=10, pady=5, sticky='w')
tkinter.Button(frame, text="Submit", command=lambda: driver(vars), font=font, bg='white', cursor='hand2')\
    .grid(columnspan=4, padx=5, pady=5)
root.mainloop()
