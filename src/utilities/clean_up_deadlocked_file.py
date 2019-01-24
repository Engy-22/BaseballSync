from utilities.dbconnect import DatabaseConnection


def clean_up_deadlocked_file():
    with open('C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\src\\utilities\\deadlocked.txt', 'r') as f:
        db = DatabaseConnection()
        for line in f:
            db.write(line)
        db.close()
