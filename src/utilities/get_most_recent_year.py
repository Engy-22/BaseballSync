from utilities.dbconnect import DatabaseConnection


def get_most_recent_year(driver_logger):
    driver_logger.log('\t* Getting most recent year listed in the database *')
    db = DatabaseConnection()
    try:
        most_recent_year = int(db.read("select year from years order by year desc limit 1;")[0][0])
    except:
        most_recent_year = 1876
    finally:
        db.close()
    return most_recent_year


# print(get_most_recent_year())
