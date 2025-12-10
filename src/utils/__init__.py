import logging
import os
from datetime import datetime

# Get the project root directory (three levels up from this file)
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
logs_dir = os.path.join(project_root, "logs")

# Make sure logs folder exists
os.makedirs(logs_dir, exist_ok=True)

log_filename = os.path.join(logs_dir, f"pipeline_{datetime.now().strftime('%Y%m%d')}.log")

# Create a named logger
logger = logging.getLogger("etl_pipeline")
logger.setLevel(logging.INFO)

# add handlers
if not logger.handlers:
    # File handler
    file_handler = logging.FileHandler(os.path.join(logs_dir, "pipeline.log"), mode="a")
    file_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    # Console handler
    console_handler = logging.StreamHandler()
    console_formatter = logging.Formatter("%(levelname)s - %(message)s")
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

logger.info("ETL Pipeline Logger initialized")
