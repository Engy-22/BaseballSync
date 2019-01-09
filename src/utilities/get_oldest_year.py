from utilities.DB_Connect import DB_Connect


def get_oldest_year(driver_logger):
    driver_logger.log('\t* Getting oldest year listed in the database *')
    db, cursor = DB_Connect.grab("baseballData")
    oldest_year = int(DB_Connect.read(cursor, "select year from years order by year limit 1;")[0][0])
    DB_Connect.close(db)
    return oldest_year


# print(get_oldest_year())
