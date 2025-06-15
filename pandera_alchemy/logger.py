import logging
import os

# Create the logger
logger = logging.getLogger("pandera_alchemy")
logger.setLevel(os.environ.get("SCHEMA_LOG_LEVEL", "DEBUG").upper())

format_str = "[%(levelname)s: %(asctime)s UTC - %(filename)s:%(lineno)s - %(funcName)s()] %(message)s"

# Set up the formatter and stream handler
formatter = logging.Formatter(format_str, datefmt="%Y-%m-%d %H:%M:%S")
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)
