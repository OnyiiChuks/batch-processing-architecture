import logging
from logging.handlers import RotatingFileHandler

# Create a logger
logger = logging.getLogger("dash_app_logger")
logger.setLevel(logging.INFO)  # Can be changed to DEBUG if needed

# Log format
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# Console logging
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
logger.addHandler(console_handler)

# File logging
file_handler = RotatingFileHandler("visualization.log", maxBytes=5*1024*1024, backupCount=3)
file_handler.setFormatter(log_formatter)
logger.addHandler(file_handler)

# Test log message
logger.info("Logger initialized successfully.")  