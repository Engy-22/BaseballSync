from utilities.database.wrappers.baseball_data_connection import DatabaseConnection
from utilities.properties import sandbox_mode, import_driver_logger as driver_logger


def get_oldest_year():
    driver_logger.log('\t* Getting oldest year listed in the database *')
    db = DatabaseConnection(sandbox_mode)
    oldest_year = int(db.read("select year from years order by year limit 1;")[0][0])
    db.close()
    return oldest_year


# print(get_oldest_year())
