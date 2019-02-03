import os
from utilities.connections.baseball_data_connection import DatabaseConnection
from concurrent.futures import ThreadPoolExecutor


db = DatabaseConnection()
with open("..\\..\\background\\table_definitions.txt", 'rt') as file:
    table_defs = file.readlines()
    print("removing existing tables")
    with ThreadPoolExecutor(os.cpu_count()) as executor:
        for line in reversed(table_defs):
            executor.submit(db.write("drop table " + line.split('create table ')[1].split(' (')[0]))
    print("creating new tables")
    with ThreadPoolExecutor(os.cpu_count()) as executor2:
        for line in table_defs:
            executor2.submit(db.write(line))
db.close()
