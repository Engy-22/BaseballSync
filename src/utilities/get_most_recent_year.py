from utilities.DB_Connect import DB_Connect


def get_most_recent_year(driver_logger):
    driver_logger.log('\t* Getting most recent year listed in the database *')
    db, cursor = DB_Connect.grab("baseballData")
    try:
        most_recent_year = int(DB_Connect.read(cursor, "select year from years order by year desc limit 1;")[0][0])
    except:
        most_recent_year = 1876
    finally:
        DB_Connect.close(db)
    return most_recent_year


# print(get_most_recent_year())
