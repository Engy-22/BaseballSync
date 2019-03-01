import tkinter
from utilities.database.wrappers.baseball_data_connection import DatabaseConnection
from utilities.database.wrappers.pitchers_pitch_fx_connection import PitcherPitchFXDatabaseConnection
from utilities.database.wrappers.batters_pitch_fx_connection import BatterPitchFXDatabaseConnection
from utilities.num_to_string import num_to_string

font = ('Times', 12)


def close():
    exit()


def gather_baseball_data_records():
    tables_records = {}
    db = DatabaseConnection(sandbox_mode=False)
    for table in db.read('show tables;'):
        tables_records[table[0]] = db.read('select * from ' + table[0] + ';')
    db.close()
    return tables_records


def gather_pitchers_pitch_fx_records():
    tables_records = {}
    db = PitcherPitchFXDatabaseConnection(sandbox_mode=False)
    for table in db.read('show tables;'):
        tables_records[table[0]] = db.read('select * from ' + table[0] + ';')
    db.close()
    return tables_records


def gather_batters_pitch_fx_records():
    tables_records = {}
    db = BatterPitchFXDatabaseConnection(sandbox_mode=False)
    for table in db.read('show tables;'):
        tables_records[table[0]] = db.read('select * from ' + table[0] + ';')
    db.close()
    return tables_records


root = tkinter.Tk()
frame = tkinter.Toplevel(root)
frame.title("Database Metrics")
total_tables = 0
total_records = 0
baseball_data_tables = 0
baseball_data_records = 0
for table, records in gather_baseball_data_records().items():
    baseball_data_tables += 1
    total_tables += 1
    for record in records:
        baseball_data_records += 1
        total_records += 1
pitchers_pitch_fx_tables = 0
pitchers_pitch_fx_records = 0
for table, records in gather_pitchers_pitch_fx_records().items():
    pitchers_pitch_fx_tables += 1
    total_tables += 1
    for record in records:
        pitchers_pitch_fx_records += 1
        total_records += 1
batters_pitch_fx_tables = 0
batters_pitch_fx_records = 0
for table, records in gather_batters_pitch_fx_records().items():
    batters_pitch_fx_tables += 1
    total_tables += 1
    for record in records:
        batters_pitch_fx_records += 1
        total_records += 1
tkinter.Label(frame, text="Metrics Dashboard", font=font).grid(columnspan=5, padx=10, pady=10)
tkinter.Label(frame, text="Tables", font=font).grid(row=2, column=0, padx=5, pady=5)
tkinter.Label(frame, text="Records", font=font).grid(row=3, column=0, padx=5, pady=5)
tkinter.Label(frame, text="baseballData", font=font).grid(row=1, column=1, padx=10, pady=10)
tkinter.Label(frame, text=str(num_to_string(baseball_data_tables)), font=font).grid(row=2, column=1, padx=5, pady=5)
tkinter.Label(frame, text=str(num_to_string(baseball_data_records)), font=font).grid(row=3, column=1, padx=5, pady=5)
tkinter.Label(frame, text="pitchers_pitch_fx", font=font).grid(row=1, column=2, padx=10, pady=10)
tkinter.Label(frame, text=str(num_to_string(pitchers_pitch_fx_tables)), font=font).grid(row=2, column=2, padx=5, pady=5)
tkinter.Label(frame, text=str(num_to_string(pitchers_pitch_fx_records)), font=font).grid(row=3, column=2, padx=5, pady=5)
tkinter.Label(frame, text="batters_pitch_fx", font=font).grid(row=1, column=3, padx=10, pady=10)
tkinter.Label(frame, text=str(num_to_string(batters_pitch_fx_tables)), font=font).grid(row=2, column=3, padx=5, pady=5)
tkinter.Label(frame, text=str(num_to_string(batters_pitch_fx_records)), font=font).grid(row=3, column=3, padx=5, pady=5)
tkinter.Label(frame, text="Total", font=font).grid(row=1, column=4, padx=10, pady=10)
tkinter.Label(frame, text=str(num_to_string(total_tables)), font=font).grid(row=2, column=4, padx=5, pady=5)
tkinter.Label(frame, text=str(num_to_string(total_records)), font=font).grid(row=3, column=4, padx=5, pady=5)
tkinter.Button(frame, text="Ok", command=lambda: close(), font=font, bg="white", width=6, cursor="hand2").\
    grid(columnspan=5, padx=10, pady=10)
root.withdraw()
root.mainloop()
