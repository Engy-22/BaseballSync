import os
import sys
import datetime
import tkinter

sys.path.append(os.path.join(sys.path[0], '..', '..', '..'))

from import_data.driver_daily import main


if __name__ == '__main__':
    if 'linux' in sys.platform:
        root = tkinter.Tk()
        root.title('Driver')
        root.withdraw()
        frame = tkinter.Toplevel(root)
        font = ('Times', 12)
        day = tkinter.IntVar()
        month = tkinter.IntVar()
        year = tkinter.IntVar()
        tkinter.Label(frame, text="Please enter the day you would \nlike to download data for:", font=font).\
            grid(columnspan=2, padx=10, pady=10)
        tkinter.Label(frame, text="Day", font=font).grid(row=1, column=0, padx=(20, 10), pady=10, sticky="E")
        tkinter.Label(frame, text="Month", font=font).grid(row=2, column=0, padx=(20, 10), pady=10, sticky="E")
        tkinter.Label(frame, text="Year", font=font).grid(row=3, column=0, padx=(20, 10), pady=10, sticky="E")
        tkinter.Entry(frame, textvariable=day, width=7).grid(row=1, column=1, padx=(10, 20), pady=10)
        tkinter.Entry(frame, textvariable=month, width=7).grid(row=2, column=1, padx=(10, 20), pady=10)
        tkinter.Entry(frame, textvariable=year, width=7).grid(row=3, column=1, padx=(10, 20), pady=10)
        tkinter.Button(frame, text="Submit", command=lambda: main(False, day.get(), month.get(), year.get(), frame),
                       font=font, cursor="hand2", bg="white").grid(columnspan=2, padx=10, pady=10)
        frame.bind("<Return>", lambda event=None: main(False, day.get(), month.get(), year.get(), frame))
        root.mainloop()
    elif 'win' in sys.platform:
        date = datetime.datetime.now() - datetime.timedelta(2)
        main(True, date.day, date.month, date.year)
    else:
        print('Unknown operating system. Must use Windows or Linux')
