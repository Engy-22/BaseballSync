from utilities.connections.baseball_data_connection import DatabaseConnection


def get_oldest_year(driver_logger, sandbox_mode):
    driver_logger.log('\t* Getting oldest year listed in the database *')
    db = DatabaseConnection(sandbox_mode)
    oldest_year = int(db.read("select year from years order by year limit 1;")[0][0])
    db.close()
    return oldest_year


# print(get_oldest_year())
