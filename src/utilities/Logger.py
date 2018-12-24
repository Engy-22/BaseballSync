import logging


class Logger:

    def __init__(self, log_file, log_level=logging.INFO):
        self.logger = logging.getLogger(log_file)
        self.logger.setLevel(log_level)
        self.file_handler = logging.FileHandler(log_file)
        self.file_handler.setLevel(log_level)
        self.logger.addHandler(self.file_handler)

    def log(self, text):
        self.logger.info(text)
