import os
import sys
import time
import datetime
import tkinter
from utilities.get_most_recent_year import get_most_recent_year
from utilities.properties import import_driver_logger as driver_logger
from import_data.driver import main

sys.path.append(os.path.join(sys.path[0], '..'))


if __name__ == '__main__':
    driver_logger.log('Begin Driver || Timestamp: ' + datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
    start_time = time.time()
    most_recent_year = get_most_recent_year()
    if 'win' in sys.platform:
        root = tkinter.Tk()
        root.title('Driver')
        root.withdraw()
        frame = tkinter.Toplevel(root)
        font = ('Times', 12)
        begin_year = tkinter.IntVar()
        end_year = tkinter.IntVar()
        tkinter.Label(frame, text="Please enter the dates you would \nlike to download data for:", font=font).\
            grid(columnspan=2, padx=10, pady=10)
        tkinter.Label(frame, text="Begin Year", font=font).grid(row=1, column=0, padx=(20, 10), pady=10, sticky="E")
        tkinter.Label(frame, text="End Year", font=font).grid(row=2, column=0, padx=(20, 10), pady=10, sticky="E")
        tkinter.Entry(frame, textvariable=begin_year, width=7).grid(row=1, column=1, padx=(10, 20), pady=10)
        tkinter.Entry(frame, textvariable=end_year, width=7).grid(row=2, column=1, padx=(10, 20), pady=10)
        tkinter.Button(frame, text="Submit", command=lambda: main(False, begin_year.get(), end_year.get(), frame),
                       font=font, cursor="hand2", bg="white").grid(columnspan=2, padx=10, pady=10)
        frame.bind("<Return>", lambda event=None: main(False, begin_year.get(), end_year.get(), frame))
        root.mainloop()
    elif 'linux' in sys.platform:
        print('\n')
        begin_year = int(input('Begin year: '))
        end_year = int(input('End year: '))
        print('\n\n\n')
        main(True, begin_year, end_year)
    else:
        print('Unknown operating system. Must use Windows or Linux')
