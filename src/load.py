# load.py - Functions to load data into PostgreSQL
# You'll build this step by step!

import psycopg2
import pandas as pd
from psycopg2 import sql
import json

# Function 01: Create the database schema
def create_tables(conn):
    pass

# Function 02: Establish connection to PostgreSQL
def get_db_connection(db_url):
    pass

# Function 03: Load valid data using UPSERT
def load_to_staging(conn, df, table_name, pk_column, batch_size=1000):
    pass

# Function 04: Load rejected records with reasons
def load_rejected(conn, rejected_df, source_name):
    pass

