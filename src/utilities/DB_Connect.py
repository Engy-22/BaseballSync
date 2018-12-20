import pymysql


class DB_Connect:
    
    def grab(db):
        db_connection = pymysql.connect("localhost","root","Invader1401asdf", db)
        return db_connection, db_connection.cursor()

    def write(db, cursor, action):
        try:
            cursor.execute(action)
            db.commit()
        except Exception as e:
            if not any(special_error in str(e) for special_error in ['Duplicate entry','check that column/key exists']):
                print('\t\t' + str(e) + ' --> ' + action)
            db.rollback()

    def read(cursor, action):
        cursor.execute(action)
        return cursor.fetchall()

    def close(db):
        db.close()
