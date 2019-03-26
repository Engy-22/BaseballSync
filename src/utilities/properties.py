import os
from utilities.logger import Logger

sandbox_mode = False
import_driver_logger = Logger(os.path.join("..", "logs", "import_data", "driver.log"))  # "..", "..", "baseball-sync",
controller_driver_logger = Logger(os.path.join("..", "logs", "controller", "main.log"))
simulsync_driver_logger = Logger(os.path.join("..", "logs", "simulsync", "trigger.log"))
