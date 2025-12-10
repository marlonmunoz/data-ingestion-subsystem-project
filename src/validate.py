# validate.py - Functions to validate and transform data
# You'll build this step by step!

import pandas as pd
import numpy as np
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from logs.utils import logger

# Function 01
def validate_required_fields(df):
    valid_mask = pd.Series([True] * len(df), index=df.index)
    all_rejects = pd.DataFrame()
    
    # Rule 1: location_id must not be null
    if 'location_id' in df.columns:
        invalid_mask = df['location_id'].isna()
        if invalid_mask.any():
            rejects = df[invalid_mask].copy()
            rejects['rejection_reason'] = 'Missing location_id'
            all_rejects = pd.concat([all_rejects,rejects], ignore_index=True)
            valid_mask = valid_mask & ~invalid_mask
    
    # Rule 2: city must not be null or empty
    if 'city' in df.columns:
        invalid_mask = (df['city'].isna()) | (df['city'].str.strip() == '')
        if invalid_mask.any():
            rejects = df[invalid_mask & valid_mask].copy()
            rejects['rejection_reason'] = 'Missing or empty city'
            all_rejects = pd.concat([all_rejects, rejects], ignore_index=True)
            valid_mask = valid_mask & ~invalid_mask
            
    # Rule 3: state must not be null or empty
    if 'state' in df.columns:
        invalid_mask = (df['state'].isna()) | (df['state'].str.strip() == '')
        if invalid_mask.any():
            rejects = df[invalid_mask & valid_mask].copy()
            rejects['rejection_reason'] = 'Missing or empty state'
            all_rejects = pd.concat([all_rejects, rejects], ignore_index=True)
            valid_mask = valid_mask & ~invalid_mask
    
    # Split into valid and invalid
    valid_df = df[valid_mask].copy()
    
    # print(f"✅ Required fields validation: {len(valid_df)} valid, {len(all_rejects)} rejected")
    logger.info(f"✅ Required fields validation: {len(valid_df)} valid, {len(all_rejects)} rejected")
    
    return valid_df, all_rejects

# Function 02   
def validate_numeric_ranges(df):
    # Start with all rows as valid  
    valid_mask = pd.Series([True] * len(df), index=df.index)
    all_rejects = pd.DataFrame()
    
    # Rule: parking_spaces must be >= 0 (if not null)
    if 'parking_spaces' in df.columns:
        # Check for negative values (but allow nulls)
        invalid_mask = (df['parking_spaces'].notna()) & (df['parking_spaces'] < 0)
        if invalid_mask.any():
            rejects = df[invalid_mask].copy()
            rejects['rejection_reason'] = 'Negative parking_spaces'
            all_rejects = pd.concat([all_rejects, rejects], ignore_index=True)
            valid_mask = valid_mask & ~invalid_mask
    
    # Split into valid and invalid
    valid_df = df[valid_mask].copy()
    
    logger.info(f"✅ Numeric range validation: {len(valid_df)} valid, {len(all_rejects)} rejected")
    
    return valid_df, all_rejects


# Function 03: Validate NULL values in important columns
def validate_null_values(df):
    # Start with all rows as valid
    valid_mask = pd.Series([True] * len(df), index=df.index)
    all_rejects = pd.DataFrame()
    
    # Define columns where NULL values should be rejected
    # You can customize this list based on your business rules
    critical_columns = [
        'data_date',        # Date field - required for time-based analysis
        'ownership_type',
        'property_type', 
        'zip_code',
        'address_line1'
    ]
    
    # Check each critical column for NULL values
    for col in critical_columns:
        if col in df.columns:
            # Find rows with NULL values in this column
            invalid_mask = df[col].isna()
            if invalid_mask.any():
                # Get only the rows that are still valid and have NULL in this column
                rejects = df[invalid_mask & valid_mask].copy()
                rejects['rejection_reason'] = f'NULL value in {col}'
                all_rejects = pd.concat([all_rejects, rejects], ignore_index=True)
                # Mark these rows as invalid
                valid_mask = valid_mask & ~invalid_mask
    
    # Split into valid and invalid
    valid_df = df[valid_mask].copy()
    
    logger.info(f"✅ NULL value validation: {len(valid_df)} valid, {len(all_rejects)} rejected")
    
    return valid_df, all_rejects


# Helper Function 04
def remove_duplicates(df, primary_key):
    original_count = len(df)
    df = df.drop_duplicates(subset=[primary_key], keep='first')
    duplicates_removed = original_count - len(df)
    
    # print(f"✅ Removed {duplicates_removed} duplicate records (kept first ocurrence)")
    logger.info(f"✅ Removed {duplicates_removed} duplicate records (kept first ocurrence)")
    
    return df


# # TEST
# if __name__=="__main__": # pragma: no cover
#     import sys
#     import os
    
#     # Add parent directory
#     sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
    
#     from readers.csv_read import read_csv
#     from clean import rename_columns, strip_whitespace, handle_missing_values
    
#     # Pipeline: Read → Clean → Validate
#     print("="*60)
#     print("STEP 1: Read and Clean Data")
#     print("="*60)
#     df = read_csv("data/real_estate.csv")
#     df = rename_columns(df)
#     df = strip_whitespace(df)
#     df = handle_missing_values(df)
    
#     print(f"\nStarting with: {len(df)} rows")
    
#     # Validate required fields
#     print("\n" + "="*60)
#     print("STEP 2: Validate Required Fields")
#     print("="*60)
#     valid_df, rejects_required = validate_required_fields(df)
    
    
#     # Validate numeric ranges
#     print("\n" + "="*60)
#     print("STEP 2: Validate Numeric Ranges")
#     print("="*60)
#     valid_df, rejects_numeric = validate_numeric_ranges(valid_df)
    
#     # Remove duplicates
#     print("\n" + "="*60)
#     print("STEP 4: Remove Duplicates")
#     print("="*60)
#     valid_df = remove_duplicates(valid_df, 'location_id')
    
#     # Combine all rejects
#     all_rejects = pd.concat([rejects_required, rejects_numeric], ignore_index=True)
    
#     # Summary
#     print("\n" + "="*60)
#     print("VALIDATION SUMMARY")
#     print("="*60)
#     print(f"✅ Valid records: {len(valid_df)}")
#     print(f"❌ Rejected records: {len(all_rejects)}")
#     print(f"📊 Success rate: {len(valid_df) / len(df) * 100:1f}%")
    
#     if len(all_rejects) > 0:
#         print("\n"+"="*60)
#         print("REJECTION REASONS:")
#         print("="*60)
#         print(all_rejects['rejection_reason'].value_counts())
        
#         print("\n"+"="*60)
#         print("SAMPLE REHJECTED RECORDS:")
#         print("="*60)
#         print(all_rejects[['location_id', 'city', 'state', 'rejection_reason']].head(5))
    
    
    
    
'''validate.py checks that records meet business rules 
(required fields, valid ranges, no duplicates), separates 
good data from bad data, and tracks rejection reasons for 
any records that fail validation.'''
    
    
    
    
    
    
    
    
    
    
    
    
    
