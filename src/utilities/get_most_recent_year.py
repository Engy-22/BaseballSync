from utilities.DB_Connect import DB_Connect
from utilities.Logger import Logger

driver_logger = Logger("C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\logs\\import_data\\driver.log")


def get_most_recent_year():
    driver_logger.log('Getting most recent year listed in the database')
    db, cursor = DB_Connect.grab("baseballData")
    most_recent_year = int(DB_Connect.read(cursor, "select year from years order by year desc limit 1;")[0][0])
    DB_Connect.close(db)
    return most_recent_year


# print(get_most_recent_year())
