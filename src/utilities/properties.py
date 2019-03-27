import os
from utilities.logger import Logger

sandbox_mode = False
if sandbox_mode:
    try:
        log_prefix = os.path.join("..", "..", "..", "logs", "sandbox")
        import_driver_logger = Logger(os.path.join(log_prefix, "import_data", "driver.log"))
        controller_driver_logger = Logger(os.path.join(log_prefix, "controller", "main.log"))
        simulsync_driver_logger = Logger(os.path.join(log_prefix, "simulsync", "trigger.log"))
    except FileNotFoundError:
        log_prefix = os.path.join("..", "logs", "production")
        import_driver_logger = Logger(os.path.join(log_prefix, "import_data", "driver.log"))
        controller_driver_logger = Logger(os.path.join(log_prefix, "controller", "main.log"))
        simulsync_driver_logger = Logger(os.path.join(log_prefix, "simulsync", "trigger.log"))
else:
    try:
        log_prefix = os.path.join("..", "..", "..", "logs", "production")
        import_driver_logger = Logger(os.path.join(log_prefix, "import_data", "driver.log"))
        controller_driver_logger = Logger(os.path.join(log_prefix, "production", "controller", "main.log"))
        simulsync_driver_logger = Logger(os.path.join(log_prefix, "production", "simulsync", "trigger.log"))
    except FileNotFoundError:
        log_prefix = os.path.join("..", "logs", "production")
        import_driver_logger = Logger(os.path.join(log_prefix, "import_data", "driver.log"))
        controller_driver_logger = Logger(os.path.join(log_prefix, "controller", "main.log"))
        simulsync_driver_logger = Logger(os.path.join(log_prefix, "simulsync", "trigger.log"))
