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
        # Get database credentials from environment variables
        # os.getenv() retrieves environment variables, with fallback default
        connection = psycopg2.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            database=os.getenv('DB_NAME', 'real_estate_db'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', 'password'),
            port=os.getenv('DB_PORT', '5432')
        )
        logging.info('Successfully connected to PostgreSQL database')
        return connection
    except Exception as e:
        logging.error(f'Failed to connect to database: {e}')
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
        ownership_type VARCHAR(20),
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
        created_at TIMESTAMP DEFAULT NOW(),  -- Set once on creation
        updated_at TIMESTAMP DEFAULT NOW(),  -- Updated on every change
        UNIQUE(location_id)
    );
    """
    
    # Reject tables for invalid records
    rejects_table_sql = """
    CREATE TABLE IF NOT EXISTS stg_rejects (
        id SERIAL PRIMARY KEY,
        source_name VARCHAR(100) NOT NULL,
        raw_data TEXT NOT NULL,
        rejection_reason TEXT NOT NULL,
        rejected_at TIMESTAMP DEFAULT NOW()
    );
    """
    
    connection = None
    try:
        # Get database connection
        connection = get_db_connection()
        cursor = connection.cursor()
        
        # Execute both CREATE TABLE statements
        cursor.execute(real_estate_table_sql)
        cursor.execute(rejects_table_sql)
        
        # Commit the changes to the database
        connection.commit()
        logging.info("Successfully created staging tables")
        
        cursor.close()
    except Exception as e:
        # if table creation fails, log the error
        logging.error(f"Failed to create tables: {e}")
        if connection:
            connection.rollback()
        raise
    finally:
        # Always close connection when done, even if there's an error
        if connection:
            connection.close()  
        
def load_to_db(df, table_name, connection):
    """
    Load DataFrame rows into the specified PostgreSQL table using UPSERT
    
    Parameters:
    - df: pandas DataFrame with data to load
    - table_name: name of the table ('stg_real_estate' or 'stg_rejects')
    - connection: active database connection
    """
    
    # If DataFrame is empty, don't try to load anything
    if df.empty:
        logging.info(f'No records to load into {table_name}')
        return 
    
    cursor = connection.cursor()
    records_loaded = 0
    try:
        # Loop thru each row in the DataFrame
        for _, row in df.iterrows():

            if table_name == 'stg_real_estate':
                # SQL for inserting into main table
                # ON CONFLICT means if location_id already exists, UPDATE instead of failing
                sql = f"""
                INSERT INTO stg_real_estate (
                    data_date, ownership_type, parking_spaces, status, 
                    property_type, congressional_district, location_id, 
                    region_id, ada_accessible, ansi_usable, city, county,
                    address_line1, state, zip_code
                ) VALUES (
                    %(data_date)s, %(ownership_type)s, %(parking_spaces)s, %(status)s, 
                    %(property_type)s, %(congressional_district)s, %(location_id)s, 
                    %(region_id)s, %(ada_accessible)s, %(ansi_usable)s,
                    %(city)s, %(county)s, %(address_line1)s, %(state)s, %(zip_code)s
                )
                ON CONFLICT (location_id) 
                DO UPDATE SET
                    data_date = EXCLUDED.data_date,
                    ownership_type = EXCLUDED.ownership_type,
                    parking_spaces = EXCLUDED.parking_spaces,
                    status = EXCLUDED.status,
                    property_type = EXCLUDED.property_type,
                    congressional_district = EXCLUDED.congressional_district,
                    region_id = EXCLUDED.region_id,
                    ada_accessible = EXCLUDED.ada_accessible,
                    ansi_usable = EXCLUDED.ansi_usable,
                    city = EXCLUDED.city,
                    county = EXCLUDED.county,
                    address_line1 = EXCLUDED.address_line1,
                    state = EXCLUDED.state,
                    zip_code = EXCLUDED.zip_code,
                    updated_at = NOW();
                """
                cursor.execute(sql, row.to_dict())
                records_loaded += 1
                
            elif table_name == 'stg_rejects':
                # SQL for inserting into rejects table
                sql = """
                INSERT INTO stg_rejects (source_name, raw_data, rejection_reason)
                VALUES (%(source_name)s, %(raw_data)s, %(rejection_reason)s);
                """
                # Prepare values for rejects table
                values = {
                    'source_name': row.get('source_name', 'unknown'),
                    'raw_data': str(row.to_dict()),  # Convert entire row to string
                    'rejection_reason': row.get('rejection_reason', 'Unknown error')
                }
                cursor.execute(sql, values)
                records_loaded += 1
            
            # Commit all the insets at once (more efficient than committing each row)
            connection.commit()
            logging.info(f"Successfully loaded {records_loaded} records into {table_name}")
    except Exception as e:
        # If anything goes wrong, undo all changes
        connection.rollback()
        logging.error(f"Failed to load data into {table_name}: {e}")
        raise
    finally:
        # Always close the cursor        
        cursor.close()
        
def drop_tables():
    """
    Drop all staging tables (useful for testing/resetting)
    WARNING: This will delete all data!
    """
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        
        cursor.execute("DROP TABLE IF EXISTS stg_real_estate CASCADE;")
        cursor.execute("DROP TABLE IF EXISTS stg_rejects CASCADE;")
        
        connection.commit()
        logging.info("Succesfully dropped staging tables")
        cursor.close()
    except Exception as e:
        logging.error(f"Failed to drop tables: {e}")
        if connection:
            connection.rollback()
        raise
    finally:
        if connection:
            connection.close()
        
    
if __name__== "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format= '%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Create tables when script is run directly
    create_tables()
    logging.info("Database setup complete")