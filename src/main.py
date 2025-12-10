# main.py - The main pipeline
# You'll build this step by step!

import sys
import os
from readers.csv_read import read_csv
from clean import rename_columns, strip_whitespace, handle_missing_values, convert_date_format
from rules import apply_all_validations
from load import get_db_connection, create_tables, load_to_staging, load_rejected
from config import load_config
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from logs.utils import logger

def run_pipeline(config_path="config/sources.json"):
    try: 
        
        logger.info("="*70)
        logger.info("START ETL PIPELINE")
        logger.info("="*70)
        
        # Load Config
        logger.info("\n[1/5] Loading configuration...")
        config = load_config(config_path)
        source_config = config['sources'][0]
        defaults = config['defaults']
        
        file_path = source_config['path']
        primary_key = source_config.get('primary_key', 'location_id')
        db_url = defaults['db_url']
        
        logger.info(f" Configuration loaded")
        logger.info(f" Source: {file_path}")
        logger.info(f" Database: {source_config['target_table']}")
        
        # Extract
        logger.info("\n[2/5] Extracting Data...")
        df = read_csv(file_path)
        logger.info(f" Extracted {len(df)} rows")
        
        # Tranform - Clean Data
        logger.info("\n[3/5] Transform Data...")
        df = rename_columns(df)
        df = strip_whitespace(df)
        df = handle_missing_values(df)
        df = convert_date_format(df)
        logger.info(f" Cleaned {len(df)} rows")
        
        # Transfor - Valid Data
        logger.info("\n[4/5] Validating Data...")
        vaild_df, rejected_df = apply_all_validations(df, primary_key=primary_key)
        logger.info(f" Validating Complete: {len(vaild_df)} valid, {len(rejected_df)} rejected")
        
        # Load to Database
        logger.info("\n[5/5] Loading to database...")
        conn = get_db_connection(db_url)
        
        try:
            
            create_tables(conn)
            
            valid_count = load_to_staging(
                conn,
                vaild_df,
                table_name=source_config['target_table'],
                pk_column=primary_key,
                batch_size=defaults['batch_size']
            )
            
            rejected_count = load_rejected(
                conn,
                rejected_df,
                source_name=source_config['name']
            )
            
            logger.info(f" Loaded {valid_count} valid records")
            logger.info(f" Loaded {rejected_count} rejected records")
        finally:
            conn.close()
            
        logger.info("\n" + "="*70)
        logger.info("ETL PIPELINE COMPLETE ✅")
        logger.info("="*70)
        logger.info(f"Total records processed: {len(df)}")
        logger.info(f"Valid records loaded: {valid_count}")
        logger.info(f"Rejected records logged: {rejected_count}")
        logger.info(f"Success rate: {(valid_count/len(df)*100):.1f}%")
        
        return valid_count, rejected_count
    except Exception as e:
        logger.error("\n" + "="*70)
        logger.error('PIPELINE FAILED')
        logger.error("="*70)
        logger.error(f"Error: {str(e)}")
        sys.exit(1)

if __name__=="__main__": # pragma: no cover
    run_pipeline()
        
        
'''Orchestrates the complete ETL pipeline by reading configuration, 
extracting CSV data, cleaning and validating it, then loading valid 
records to PostgreSQL while logging any rejected records.'''
        