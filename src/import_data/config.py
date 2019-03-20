import json
import os

try:
    with open('/etc/config.json') as config_file:
        config = json.load(config_file)
    from_server = True
except FileNotFoundError:
    from_server = False


class Config:
    if from_server:
        MAIN_DB = str(config.get('MAIN_DB'))
        BRANCH_DB_P = str(config.get('BRANCH_DB_P'))
        BRANCH_DB_B = str(config.get('BRANCH_DB_B'))
        MYSQL_PASS = str(config.get('MYSQL_PASS'))
        MYSQL_HOST = str(config.get('MYSQL_HOST'))
        MYSQL_USER = str(config.get('MYSQL_USER'))
    else:
        MAIN_DB = str(os.environ.get('MAIN_DB'))
        BRANCH_DB_P = str(os.environ.get('BRANCH_DB_P'))
        BRANCH_DB_B = str(os.environ.get('BRANCH_DB_B'))
        MYSQL_PASS = str(os.environ.get('MYSQL_PASS'))
        MYSQL_HOST = str(os.environ.get('MYSQL_HOST'))
        MYSQL_USER = str(os.environ.get('MYSQL_USER'))
