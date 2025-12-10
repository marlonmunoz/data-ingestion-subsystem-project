# load.py - Functions to load data into PostgreSQL
# You'll build this step by step!

import psycopg2
import pandas as pd
from psycopg2 import sql
import json
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from logs.utils import logger

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
        logger.info("✅ Tables created successfully (or already created)")
    except Exception as e:
        conn.rollback()
        logger.error(f"❌ Error creating tables: {e}")
        raise
    finally:
        cursor.close()



# Function 02: Establish connection to PostgreSQL
def get_db_connection(db_url):
    try:
        conn = psycopg2.connect(db_url)
        logger.info("Connected to database successfully")
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise

# Function 03: Load valid data using UPSERT
def load_to_staging(conn, df, table_name, pk_column, batch_size=1000):
    cursor = conn.cursor()
    columns = df.columns.tolist()
    
    placeholders = ', '.join(['%s'] * len(columns))
    columns_str = ', '.join(columns)
    
    update_cols = [col for col in columns if col != pk_column]
    update_str = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_cols])
    
    upsert_query = f"""
        INSERT INTO {table_name} ({columns_str})
        VALUES ({placeholders})
        ON CONFLICT ({pk_column}) DO UPDATE SET
        {update_str}
    """
    
    try: 
        total_rows = 0
        
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i+batch_size]
            
            for _, row in batch.iterrows():
                
                values = []
                for col in columns:
                    val = row[col]
                    
                    if pd.isna(val):
                        values.append(None)
                    else:
                        values.append(val)
                cursor.execute(upsert_query, values)
                total_rows += 1
            conn.commit()
            logger.info(f" Loaded batch: {min(i+batch_size, len(df))}")
        
        logger.info(f"Successfully loaded {total_rows} rows to {table_name}")
        return total_rows
    
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading data: {e}")
        raise
    finally:
        cursor.close()
        

# Function 04: Load rejected records with reasons
def load_rejected(conn, rejected_df, source_name):
    if len(rejected_df) == 0:
        logger.info("No rejecet records to load")
        return 0
    
    cursor = conn.cursor()
    
    insert_query = """
        INSERT INTO stg_rejects (source_name, raw_data, rejection_reason)
        VALUES (%s, %s, %s)
    """
    
    try: 
        for _, row in rejected_df.iterrows():
            row_dict = row.drop('rejection_reason').to_dict()
            raw_data_json = json.dumps(row_dict, default=str)
            
            cursor.execute(insert_query, (
                source_name,
                raw_data_json,
                row['rejection_reason']
            ))
        conn.commit()
        logger.info(f"Loaded {len(rejected_df)} rejected to stg_rejects")
        return len(rejected_df)
    except Exception as e:
        conn.rollback()
        logger.error(f"Error Loading rejects: {e}")
        raise
    finally:
        cursor.close()
        

# # TEST
# if __name__=="__main__": # pragma: no cover
#     import sys
#     import os
    
#     sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
    
#     from readers.csv_read import read_csv
#     from clean import rename_columns, strip_whitespace, handle_missing_values, convert_date_format
#     from rules import apply_all_validations
    
#     DB_URL = "postgresql://postgres:password@localhost:5432/real_estate_db"
    
#     print("="*60)
#     print("FULL ETL PIPELIN EWITH DATABASE LOADING")
#     print("="*60)
    
    
#     print("\nStep 1: Reading and Cleaning Data...")
#     df = read_csv("data/real_estate.csv")
#     df = rename_columns(df)
#     df = strip_whitespace(df)
#     df = handle_missing_values(df)
#     df = convert_date_format(df)
    
    
#     print("\n Step 2: Validating Data...")
#     valid_df, rejected_df = apply_all_validations(df, primary_key='location_id')
    
#     print("\n Step 3: Connecting to Database...")
#     conn = get_db_connection(DB_URL)
    
#     print("\n Step 4: Creating Tables...")
#     create_tables(conn)
    
#     print("\n Step 5: Loading Valid Data...")
#     load_to_staging(conn, valid_df, 'stg_real_estate', 'location_id', batch_size=1000)
    
#     print("\n Step 6: Loading Rejected Data...")
#     load_rejected(conn, rejected_df, 'real_estate_csv')
    
#     conn.close()
    
#     print("\n" + "="*60)
#     print("ETL PIPELINE COMPLETE!")
#     print("="*60)
#     print(f"Valid records loaded: {len(valid_df)}")
#     print(f"rejected records logged: {len(rejected_df)}")
    
    
    '''load.py
connects to PostgreSQL, creates staging tables, and uses batch 
UPSERT to load validated real estate records into stg_real_estate 
while logging any rejected records with their errors as JSONB in 
stg_rejects.'''
    

