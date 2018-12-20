from utilities.DB_Connect import DB_Connect


db, cursor = DB_Connect.grab("baseballData")
with open("..\\..\\background\\table_definitions.txt", 'rt') as file:
    table_defs = file.readlines()
    print("removing existing tables")
    for line in reversed(table_defs):
        DB_Connect.write(db, cursor, "drop table " + line.split('create table ')[1].split(' (')[0])
    print("creating new tables")
    for line in table_defs:
        DB_Connect.write(db, cursor, line)
DB_Connect.close(db)
