import os
import datetime
import smtplib
from import_data.config import Config as config


def send_results():
    sender = config.MAIL_USERNAME
    recipient = config.MAIL_RECIPIENT
    pwd = config.MAIL_PASSWORD
    header = "To: " + recipient + '\nFrom: ' + sender + '\nSubject: Daily download results'
    s = smtplib.SMTP(config.MAIL_SERVER, config.MAIL_PORT)
    s.ehlo()
    s.starttls()
    s.ehlo()
    s.login(sender, pwd)
    s.sendmail(sender, recipient, header + '\n\n' + get_results())
    s.quit()


def get_results():
    body = ''
    info_is_now_relevant = False
    with open(os.path.join("..", "..", "logs", "import_data", "driver.log"), 'rt') as log_file:
        for line in log_file.readlines():
            if info_is_now_relevant or datetime.datetime.today().strftime('%Y-%m-%d') in line:
                info_is_now_relevant = True
            else:
                continue
            body += line
    return body


send_results()
