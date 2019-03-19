import os
from utilities.logger import Logger

sandbox_mode = True
import_driver_logger = Logger(os.path.join("..", "..", "logs", "import_data", "driver.log"))
controller_driver_logger = Logger(os.path.join("..", "..", "logs", "controller", "main.log"))
