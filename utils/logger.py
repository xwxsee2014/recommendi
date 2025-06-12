from loguru import logger
import os
import sys

LOG_FILE = "ai-content-generation.log"
ROTATION_TIME = "00:00"
LOG = logger

def set_logger_config(log_dir="logs", filename = LOG_FILE, debug=False):
    global LOG
    LOG.remove()
    level = "DEBUG" if debug else "INFO"
    LOG.add(sys.stdout, level=level)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    log_file_path = os.path.join(log_dir, filename)
    LOG.add(log_file_path, rotation=ROTATION_TIME, level=level)

class Logger:
    def __init__(self, log_dir="logs", filename = LOG_FILE, debug=False):
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        log_file_path = os.path.join(log_dir, filename)

        # Remove default loguru handler
        logger.remove()

        # Add console handler with a specific log level
        level = "DEBUG" if debug else "INFO"
        logger.add(sys.stdout, level=level)
        # Add file handler with a specific log level and timed rotation
        logger.add(log_file_path, rotation=ROTATION_TIME, level="INFO")
        self.logger = logger

if __name__ == "__main__":
    log = Logger().logger

    log.debug("This is a debug message.")
    log.info("This is an info message.")
    log.warning("This is a warning message.")
    log.error("This is an error message.")
