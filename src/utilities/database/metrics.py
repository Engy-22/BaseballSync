import os
import sys
if 'linux' in sys.platform:
    sys.path.append(os.path.join('/home', 'araimond', 'baseball-sync', 'src'))
import tkinter
from wrappers.baseball_data_connection import DatabaseConnection
from wrappers.pitchers_pitch_fx_connection import PitcherPitchFXDatabaseConnection
from wrappers.batters_pitch_fx_connection import BatterPitchFXDatabaseConnection
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


def create_table_for_linux():
    def calculate_spaces(length_of_number, length_of_header):
        spaces = ''
        for _ in range((length_of_header-length_of_number)):
            spaces += ' '
        return spaces
    bdh = 'baseballData'
    ppfxh = 'pitchers_pitch_fx'
    bpfxh = 'batters_pitch_fx'
    th = 'Total'
    tt = str(total_tables)
    tr = num_to_string(total_records)
    bdt = str(baseball_data_tables)
    bdr = num_to_string(baseball_data_records)
    ppfxt = str(pitchers_pitch_fx_tables)
    ppfxr = num_to_string(pitchers_pitch_fx_records)
    bpfxt = str(batters_pitch_fx_tables)
    bpfxr = num_to_string(batters_pitch_fx_records)
    beginning_spaces = calculate_spaces(0, len('Records  '))
    length = ''
    for _ in range(len(bdh)+len(ppfxh)+len(bpfxh)+32):
        length += '-'
    print('\n' + beginning_spaces + length + '\n' + beginning_spaces + '|  ' + bdh + '  |  ' + ppfxh + '  |  ' + bpfxh
          + '  |     ' + th + '     |\n' + beginning_spaces + length + '\nTables   |  ' + bdt
          + calculate_spaces(len(bdt), len(bdh)) + '  |  ' + ppfxt + calculate_spaces(len(ppfxt), len(ppfxh)) + '  |  '
          + bpfxt + calculate_spaces(len(bpfxt), len(bpfxh)) + '  |  ' + tt + calculate_spaces(len(tt), 11) + '  |\n'
          + beginning_spaces + length + '\nRecords  |  ' + bdr + calculate_spaces(len(bdr), len(bdh)) + '  |  ' + ppfxr
          + calculate_spaces(len(ppfxr), len(ppfxh)) + '  |  ' + bpfxr + calculate_spaces(len(bpfxr), len(bpfxh))
          + '  |  ' + tr + calculate_spaces(len(tr), 11) + '  |  \n' + beginning_spaces + length + '\n')


if __name__ == '__main__':
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
    if 'win' in sys.platform:
        root = tkinter.Tk()
        frame = tkinter.Toplevel(root)
        frame.title("Database Metrics")
        tkinter.Label(frame, text="Metrics Dashboard", font=font).grid(columnspan=5, padx=10, pady=10)
        tkinter.Label(frame, text="Table Count", font=font).grid(row=2, column=0, padx=5, pady=5, sticky="W")
        tkinter.Label(frame, text="Record Count", font=font).grid(row=3, column=0, padx=5, pady=5, sticky="W")
        tkinter.Label(frame, text="baseballData", font=font).grid(row=1, column=1, padx=10, pady=10)
        tkinter.Label(frame, text=str(num_to_string(baseball_data_tables)), font=font).grid(row=2, column=1, padx=5,
                                                                                            pady=5)
        tkinter.Label(frame, text=str(num_to_string(baseball_data_records)), font=font).grid(row=3, column=1, padx=5,
                                                                                             pady=5)
        tkinter.Label(frame, text="pitchers_pitch_fx", font=font).grid(row=1, column=2, padx=10, pady=10)
        tkinter.Label(frame, text=str(num_to_string(pitchers_pitch_fx_tables)), font=font).grid(row=2, column=2, padx=5,
                                                                                                pady=5)
        tkinter.Label(frame, text=str(num_to_string(pitchers_pitch_fx_records)), font=font).grid(row=3, column=2,
                                                                                                 padx=5, pady=5)
        tkinter.Label(frame, text="batters_pitch_fx", font=font).grid(row=1, column=3, padx=10, pady=10)
        tkinter.Label(frame, text=str(num_to_string(batters_pitch_fx_tables)), font=font).grid(row=2, column=3, padx=5,
                                                                                               pady=5)
        tkinter.Label(frame, text=str(num_to_string(batters_pitch_fx_records)), font=font).grid(row=3, column=3, padx=5,
                                                                                                pady=5)
        tkinter.Label(frame, text="Total", font=font).grid(row=1, column=4, padx=10, pady=10)
        tkinter.Label(frame, text=str(num_to_string(total_tables)), font=font).grid(row=2, column=4, padx=5, pady=5)
        tkinter.Label(frame, text=str(num_to_string(total_records)), font=font).grid(row=3, column=4, padx=5, pady=5)
        tkinter.Button(frame, text="Ok", command=lambda: close(), font=font, bg="white", width=6, cursor="hand2").\
            grid(columnspan=5, padx=10, pady=10)
        root.withdraw()
        root.mainloop()
    elif 'linux' in sys.platform:
        create_table_for_linux()
    else:
        print('Unknown operating system. Must use Windows or Linux')
