from utilities.database.wrappers.baseball_data_connection import DatabaseConnection


def get_most_recent_year():
    db = DatabaseConnection(sandbox_mode=False)
    try:
        most_recent_year = int(db.read("select year from years order by year desc limit 1;")[0][0])
    except:
        most_recent_year = 1876
    finally:
        db.close()
    return most_recent_year
