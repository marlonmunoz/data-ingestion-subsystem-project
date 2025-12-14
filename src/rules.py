'''           Validation Orchestrator

  1. Orchestration = Combining multiple functions into one workflow
  2. Single Responsability = Each function does one thing well
  3. Facade Pattern: Simple interface for complex operations
  4. Return Multiple Values: Using tuples in Python 
'''

import pandas as pd
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from utils import logger
from validate import validate_required_fields, validate_numeric_ranges, validate_null_values, remove_duplicates


# MASTER FUNCTION 
def apply_all_validations(df, primary_key='location_id'):
    
    logger.info("\n" + "="*60)
    logger.info("STARTING VALIDATION PIPELINE")
    logger.info("="*60)
    logger.info(f"Input records: {len(df)}")
    
    # Track all rejects across all validations steps
    all_rejects = pd.DataFrame()
    
    # Step 01: Validate required fields (columns)
    logger.info("\n Validation required fields...")
    valid_df, rejects_required = validate_required_fields(df)
    if len(rejects_required) > 0:
        all_rejects = pd.concat([all_rejects, rejects_required], ignore_index=True)
        
        
    # Step 02: Validate numeric ranges (only on valid data so far)
    logger.info("\n Validation numeric ranges...")
    valid_df, rejects_numeric = validate_numeric_ranges(valid_df)
    if len(rejects_numeric) > 0:
        all_rejects = pd.concat([all_rejects, rejects_numeric], ignore_index=True)
    
    
    # Step 03: Validate NULL values in critical columns
    logger.info("\n Validating NULL values...")
    valid_df, rejects_nulls = validate_null_values(valid_df)
    if len(rejects_nulls) > 0:
        all_rejects = pd.concat([all_rejects, rejects_nulls], ignore_index=True)
    
    
    # Step 04: Remove duplicates (only from valid data)
    logger.info("\n Removing Duplicates...")
    valid_df = remove_duplicates(valid_df, primary_key)
    
    
    # Summary
    logger.info("\n" + "="*60)
    logger.info("VALIDATION COMPLETE")
    logger.info("="*60)
    logger.info(f"✅ Valid records: {len(valid_df)} ")
    logger.info(f"❌ Rejected records: {len(all_rejects)}")
    
    if len(df) > 0:
        success_rate = (len(valid_df) / len(df) * 100)
        logger.info(f"Success Rate: {success_rate:.1f}%")
    
    return valid_df, all_rejects


# TEST 
if __name__=="__main__": # pragma: no cover
    import sys
    import os
    
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
    
    from readers.csv_read import read_csv
    from clean import rename_columns, strip_whitespace, handle_missing_values, convert_date_format
    
    # Full pipeline: Read → Clean → Validate
    
    print("\n" + "="*60)
    print("FULL DATA PIPELINE")
    print("="*60)
    
    
    # S1: READ DATA
    print("\n Reading CSV..." )
    df = read_csv("data/real_estate.csv")
    
    # S2: CLEAN DATA
    print("\n Cleaning data CSV..." )
    df = rename_columns(df)
    df = strip_whitespace(df)
    df = handle_missing_values(df)
    df = convert_date_format(df)  # Convert dates to proper format
    
    # S3#: Validate with new Orchestrator
    valid_df, rejected_df = apply_all_validations(df, primary_key="location_id")   
    
    # RESULTS
    print("\n" + "="*60)
    print("FINAL RESULTS")
    print("="*60)
    print(f"Valid records ready for database: {len(valid_df)}")
    print(f"Rejected records to review: {len(rejected_df)}")
    
    if len(rejected_df) > 0:
        print("\n" + "="*60)
        print("REJECTION BREAKDOWN:")
        print("="*60)
        print(rejected_df['rejection_reason'].value_counts())
    
    print("\n" + "="*60)
    print("SAMPLE VALID DATA (First 3 rows):")
    print("="*60)
    print(valid_df.head(3))
        
    
    
'''rules.py orchestrates all validation functions 
(required fields, numeric ranges, deduplication) into 
a single function call that returns clean valid data 
and tracked rejected records.'''
    