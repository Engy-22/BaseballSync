import os
from utilities.connections.baseball_data_connection import DatabaseConnection
from concurrent.futures import ThreadPoolExecutor


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
            executor.submit(db.write("drop table " + line.split('create table ')[1].split(' (')[0]))
    if sandbox_mode:
        print("creating new tables - sandbox")
    else:
        print("creating new tables - production")
    with ThreadPoolExecutor(os.cpu_count()) as executor2:
        for line in table_defs:
            executor2.submit(db.write(line))
db.close()
