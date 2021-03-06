import os
from utilities.database.wrappers.baseball_data_connection import DatabaseConnection
from utilities.properties import sandbox_mode, import_driver_logger as driver_logger


def clean_up_deadlocked_file():
    driver_logger.log("\tCleaning up deadlocked records")
    with open(os.path.join("utilities", "deadlocked.txt"), 'r') as f:
        db = DatabaseConnection(sandbox_mode)
        for line in f:
            db.write(line)
        db.close()
    file = open(os.path.join("utilities", "deadlocked.txt"), "w").close()
