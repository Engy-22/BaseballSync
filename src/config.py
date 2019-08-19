import json
import os

try:
    with open("/etc/config.json") as config_file:
        config = json.load(config_file)
    FROM_SERVER = True
except FileNotFoundError:
    FROM_SERVER = False


class Config:
    if FROM_SERVER:
        MAIN_DB = str(config.get("MAIN_DB"))
        BRANCH_DB = str(config.get("BRANCH_DB"))
        MYSQL_PASS = str(config.get("MYSQL_PASS"))
        MYSQL_HOST = str(config.get("MYSQL_HOST"))
        MYSQL_USER = str(config.get("MYSQL_USER"))
        MAIL_USERNAME = str(config.get("EMAIL_USER"))
        MAIL_PASSWORD = str(config.get("EMAIL_PASS"))
        MAIL_RECIPIENT = str(config.get("DIAGNOSTIC_EMAIL"))
    else:
        MAIN_DB = str(os.environ.get("MAIN_DB"))
        BRANCH_DB = str(os.environ.get("BRANCH_DB"))
        MYSQL_PASS = str(os.environ.get("MYSQL_PASS"))
        MYSQL_HOST = str(os.environ.get("MYSQL_HOST"))
        MYSQL_USER = str(os.environ.get("MYSQL_USER"))
        MAIL_USERNAME = str(os.environ.get("EMAIL_USER"))
        MAIL_PASSWORD = str(os.environ.get("EMAIL_PASS"))
        MAIL_RECIPIENT = str(os.environ.get("DIAGNOSTIC_EMAIL"))
    MAIL_SERVER = "smtp.googlemail.com"
    MAIL_PORT = 587
    MAIL_USE_TLS = True
