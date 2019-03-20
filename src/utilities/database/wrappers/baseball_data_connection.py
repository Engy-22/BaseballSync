import os
import pymysql
from import_data.config import Config as config


class DatabaseConnection:
    def __init__(self, sandbox_mode):
        if sandbox_mode:
            self.db = pymysql.connect(config.MYSQL_HOST, config.MYSQL_USER, config.MYSQL_PASS,
                                      config.MAIN_DB + "_sandbox")
        else:
            self.db = pymysql.connect(config.MYSQL_HOST, config.MYSQL_USER, config.MYSQL_PASS, config.MAIN_DB)
        self.cursor = self.db.cursor()

    def write(self, action):
        try:
            self.cursor.execute(action)
            self.db.commit()
        except Exception as e:
            if not any(special_error in str(e) for special_error in ['Duplicate entry',
                                                                     'check that column/key exists']):
                print('\t\t' + str(e) + ' --> ' + action)
            elif 'deadlock' in str(e):
                with open(os.path.join("..", "..", "deadlocked.txt"), "a") as deadlocked_file:
                    deadlocked_file.write(action)
            self.db.rollback()

    def read(self, action):
        self.cursor.execute(action)
        return self.cursor.fetchall()

    def close(self):
        self.db.close()
