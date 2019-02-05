import os
import time
from utilities.connections.baseball_data_connection import DatabaseConnection
from utilities.connections.pitchers_pitch_fx_connection import PitcherPitchFXDatabaseConnection
from utilities.connections.batters_pitch_fx_connection import BatterPitchFXDatabaseConnection
from concurrent.futures import ThreadPoolExecutor
from utilities.time_converter import time_converter

start_time = time.time()
sandbox_mode = True
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
    if sandbox_mode:
        print("creating new pitcher pitch fx tables - sandbox")
    else:
        print("creating new pitcher pitch fx tables - production")
    with ThreadPoolExecutor(os.cpu_count()) as executor2:
        for line in table_defs:
            executor2.submit(db.write(line))
db.close()

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
    if sandbox_mode:
        print("creating new batter pitch fx tables - sandbox")
    else:
        print("creating new batther pitch fx tables - production")
    with ThreadPoolExecutor(os.cpu_count()) as executor2:
        for line in table_defs:
            executor2.submit(db.write(line))
db.close()
time_converter(time.time() - start_time)
