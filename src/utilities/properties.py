import os
from utilities.logger import Logger

sandbox_mode = False
import_driver_logger = Logger(os.path.join("..", "..", "baseball-sync", "logs", "import_data", "driver.log"))
controller_driver_logger = Logger(os.path.join("..", "..", "baseball-sync", "logs", "controller", "main.log"))
