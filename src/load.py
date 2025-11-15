import psycopg2 # PostgreSQL adaptor
import pandas as pd # Data manipulation
import os # Environment variable for DB credentials
from datetime import datetime # Timestamp tracking 
import logging # Track what's happening


def get_db_connection():
    """
    Create and return a PostgreSQL database connection.
    Uses environment  variables for credentials with sensible defaults
    """
    try:
        connection = psycopg2.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            databasr=os.getenv('DB_NAME', 'real_state_db'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', 'password'),
            port=os.getenv('DB_PORT', '5432')
        )
        logging.info('Database connection successful')
        return connection
    except Exception as e:
        logging.error(f'Database connection failed: {e}')
        raise