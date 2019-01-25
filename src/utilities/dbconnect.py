import pymysql


class DatabaseConnection:
    def __init__(self):
        self.db = pymysql.connect("localhost", "root", "Invader1401asdf", "baseballData")
        self.cursor = self.db.cursor()

    def write(self, action):
        try:
            self.cursor.execute(action)
            self.db.commit()
        except Exception as e:
            if not any(special_error in str(e) for special_error in ['Duplicate entry','check that column/key exists']):
                print('\t\t' + str(e) + ' --> ' + action)
            elif 'deadlock' in str(e):
                with open('C:\\Users\\Anthony Raimondo\\PycharmProjects\\baseball-sync\\src\\utilities\\deadlocked.txt',
                          'a') as deadlocked_file:
                    deadlocked_file.write(action)
            self.db.rollback()

    def read(self, action):
        self.cursor.execute(action)
        return self.cursor.fetchall()

    def close(self):
        self.db.close()
