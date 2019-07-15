import os
import datetime
import smtplib
from config import Config as config
from utilities.properties import log_prefix, import_driver_logger as driver_logger


def send_results():
    print('Emailing results')
    driver_logger.log('\tEmailing results\n\n\n\n')
    sender = config.MAIL_USERNAME
    recipient = config.MAIL_RECIPIENT
    pwd = config.MAIL_PASSWORD
    header = "To: " + recipient + '\nFrom: ' + sender + '\nSubject: Daily download results'
    s = smtplib.SMTP(config.MAIL_SERVER, config.MAIL_PORT)
    s.ehlo()
    s.starttls()
    s.ehlo()
    s.login(sender, pwd)
    s.sendmail(sender, recipient, header + '\n\n' + get_csv_results() + '\n' + get_driver_results())
    s.quit()


def get_driver_results():
    results = ''
    info_is_now_relevant = False
    with open(os.path.join(log_prefix, "import_data", "driver.log"), 'rt') as log_file:
        for line in log_file.readlines():
            if info_is_now_relevant or datetime.datetime.today().strftime('%Y-%m-%d') in line:
                info_is_now_relevant = True
            else:
                continue
            if 'ERROR' in line:
                results += line + '\n'
            elif 'Time taken' in line:
                results += 'Successful download - ' + line
    return results


def get_csv_results():
    results = ''
    with open(os.path.join("..", "src", "import_data", "player_data", "pitch_fx", "multiple_players.csv")) as file:
        for row in file:
            results += row
        if len(results) > 0:
            return "Some pitch_fx players were not found, check multiple_players.csv."
        else:
            return "All pitch_fx players were found."
