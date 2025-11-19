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
            database=os.getenv('DB_NAME', 'real_estate_db'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', 'password'),
            port=os.getenv('DB_PORT', '5432')
        )
        logging.info('Database connection successful')
        return connection
    except Exception as e:
        logging.error(f'Database connection failed: {e}')
        raise
    
# SQL Table
def create_tables():
    """
    Create the staging tables for real estate data and rejects
    """
    # This will be my main real estate table 
    real_estate_table_sql = """
    CREATE TABLE IF NOT EXISTS stg_real_estate (
        id SERIAL PRIMARY KEY,
        data_date DATE,
        ownership_type VARCHAR(20),  -- 'OWNED' or 'LEASED'
        parking_spaces INTEGER, 
        status VARCHAR(20),
        property_type VARCHAR(50),
        congressional_district INTEGER,
        location_id VARCHAR(20),
        region_id INTEGER,
        ada_accessible VARCHAR(50),
        ansi_usable VARCHAR(50),
        city VARCHAR(100),
        county VARCHAR(100),
        address_line1 VARCHAR(200),
        state VARCHAR(5),
        zip_code VARCHAR(15),
        created_at TIMESTAMP DEFAULT NOW(),
        UNIQUE(location_id)  -- Prevent duplicate properties
    );
    """
    
    # Reject tables for invalid records
    rejects_table_sql = """
    CREATE TABLE IF NOT EXISTS stg_rejects (
        id SERIAL PRIMARY KEY,
        source_name VARCHAR(100) NOT NULL,
        raw_data JSONB NOT NULL,
        rejection_reason TEXT NOT NULL,
        rejected_at TIMESTAMP DEFAULT NOW()
    );
    """
    
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        
        # Execute table creation
        cursor.execute(real_estate_table_sql)
        cursor.execute(rejects_table_sql)
        
        connection.commit()
        logging.info("Tables created successfully")
    except Exception as e:
        if connection:
            connection.rollback()
        logging.error(f"Table creation failed: {e}")
        raise
    finally:
        if connection:
            cursor.close()
            connection.close()  
        
def load_to_db(df, table_name, connection):
    """
    Load DataFrame rows into the specified PostgreSQL table using UPSERT
    """
    cursor = connection.cursor()
    for _, row in df.iterrows():
        sql = f"""
        INSERT INTO {table_name} (
            data_date, ownership_type, parking_spaces, status, property_type,
            congressional_district, location_id, region_id, ada_accessible, ansi_usable,
            city, county, address_line1, state, zip_code, created_at
        ) VALUES (
            %(data_date)s, %(ownership_type)s, %(parking_spaces)s, %(status)s, %(property_type)s,
            %(congressional_district)s, %(location_id)s, %(region_id)s, %(ada_accessible)s, %(ansi_usable)s,
            %(city)s, %(county)s, %(address_line1)s, %(state)s, %(zip_code)s, NOW()
        )
        ON CONFLICT (location_id) DO UPDATE SET
            data_date = EXCLUDED.data_date,
            ownership_type = EXCLUDED.ownership_type,
            parking_spaces = EXCLUDED.parking_spaces,
            status = EXCLUDED.status,
            property_type = EXCLUDED.property_type,
            congressional_district = EXCLUDED.congressional_district,
            region_id = EXCLUDED.region_id,
            ada_accessible = EXCLUDED.ada_accessible,
            ansi_usable =  EXCLUDED.ansi_usable,
            county = EXCLUDED.county,
            address_line1 = EXCLUDED.address_line1,
            state = EXCLUDED.state,
            zip_code = EXCLUDED.zip_code,
            created_at = NOW();
        """
        cursor.execute(sql, row.to_dict())
    connection.commit()
    cursor.close()
    
if __name__== "__main__":
    logging.basicConfig(level=logging.INFO)
    create_tables()
        