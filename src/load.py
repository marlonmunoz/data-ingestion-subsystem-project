# load.py - Functions to load data into PostgreSQL
# You'll build this step by step!

import psycopg2
import pandas as pd
from psycopg2 import sql
import json

# Function 01: Create the database schema
def create_tables(conn):
    cursor = conn.cursor()
    
    # TABLE 1: Main staging table for real estate data
    create_stg_real_estate = """
    CREATE TABLE IF NOT EXISTS stg_real_estate (
        data_date DATE,
        ownership_type VARCHAR(20),
        parking_spaces INTEGER,
        status VARCHAR(20),
        property_type VARCHAR(50),
        congressional_district VARCHAR(10),
        location_id VARCHAR(20) PRIMARY KEY,
        region_id INTEGER,
        ada_accessible VARCHAR(50),
        ansi_usable VARCHAR(50),
        city VARCHAR(100),
        county VARCHAR(100),
        address_line1 VARCHAR(200),
        state VARCHAR(5),
        zip_code VARCHAR(15),
        created_at TIMESTAMP DEFAULT NOW()
    );
    """
    
    # TABLE 2: Reject table for invalid data
    create_stg_rejects = """
    CREATE TABLE IF NOT EXISTS stg_rejects (
        id SERIAL PRIMARY KEY,
        source_name VARCHAR(100) NOT NULL,
        raw_data JSONB NOT NULL,
        rejection_reason TEXT NOT NULL,
        rejected_at TIMESTAMP DEFAULT NOW()
    );
    """
    
    try:
        cursor.execute(create_stg_real_estate)
        cursor.execute(create_stg_rejects)
        conn.commit()
        print("✅ Tables created successfully (or already created)")
    except Exception as e:
        conn.rollback()
        print(f"❌ Error creating tables: {e}")
        raise
    finally:
        cursor.close()



# Function 02: Establish connection to PostgreSQL
def get_db_connection(db_url):
    pass

# Function 03: Load valid data using UPSERT
def load_to_staging(conn, df, table_name, pk_column, batch_size=1000):
    pass

# Function 04: Load rejected records with reasons
def load_rejected(conn, rejected_df, source_name):
    pass

