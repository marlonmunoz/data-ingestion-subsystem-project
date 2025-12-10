# clean.py - Functions to clean and standardize data
# You'll build this step by step!

import pandas as pd
import re
import sys
import os

# Add parent directory to path for logs import
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from utils import logger

# Function 01
    # Makes names database-friendly(no dots/spaces)
def rename_columns(df):
    column_mapping = {
    # Date fields
        "data.date": "data_date",
    # Property details
        "data.owned or leased": "ownership_type",
        "data.parking spaces": "parking_spaces",
        "data.status": "status",
        "data.type": "property_type",
    # Location info
        "location.congressional district": "congressional_district",
        "location.id": "location_id",
        "location.region id": "region_id",
    # Accessibility info
        "data.disabilities.ADA Accessible": "ada_accessible",
        "data.disabilities.ansi usable": "ansi_usable",
    # Address fields
        "location.address.city": "city",
        "location.address.county": "county",
        "location.address.line 1": "address_line1",
        "location.address.state": "state",
        "location.address.zip": "zip_code",
    }
    logger.info(f"✅ Renamed all columns")
    return df.rename(columns=column_mapping)


# Function 02
    # Removes extra spaces that causes duplicates
def strip_whitespace(df):
    # Loop thru each column
    for col in df.columns:
        # Check if this column contains text (not numbers)
        if df[col].dtype == "object":
            # Remove spaces from ALL values in this column
            df[col] = df[col].str.strip()
            
    logger.info(f"✅ Stripped whitespace from string columns")
    return df


# Function 03 
    # Coverts fake values like 0 to proper NULL
def handle_missing_values(df):
    # Check if data_name column exists (after renaming)
    if 'data_date' in df.columns:
        # Replace the string "0" with None (Python's NULL)
        df['data_date'] = df['data_date'].replace('0', None)
        
    logger.info(f"✅ Handled missing values")
    return df

# Function 03

def convert_date_format(df):
    if 'data_date' in df.columns:
        df['data_date'] = pd.to_datetime(
            df['data_date'],
            errors='coerce'
        )
        
        null_count = df['data_date'].isna().sum()
        valid_count = df['data_date'].notna().sum()
        logger.info(f"✅ Coverted data_date format: {valid_count} valid, {null_count} null")
    return df

# if __name__=="__main__": # pragma: no cover
#     # Import the CSV reader
#     import sys
#     import os
    
#     # Add parent directory to path so we can import from readers/
#     sys.path.insert(0, os.path.join(os.path.dirname(__file__),'..'))
    
#     from readers.csv_read import read_csv
    
#     # 1. Read the CSV
#     print("="*60)
#     print("STEP 1: Reading CSV")
#     print("="*60)
#     df = read_csv("data/real_estate.csv")
    
#     # 2. Show Original column names
#     print("\n" + "="*60)
#     print("ORIGINAL COLUMNS (Before cleaning)")
#     print("="*60)
#     print(list(df.columns))
    
#     # 3. Rename columns
#     print("\n" + "="*60)
#     print("STEP 2: Renaming Columns")
#     print("="*60)
#     df = rename_columns(df)
#     print("New Columns:", list(df.columns))
    
#     # 4. Strip whitespace
#     print("\n" + "="*60)
#     print("STEP 3: Strippind Whitespace")
#     print("="*60)
#     df = strip_whitespace(df)
    
#     # 5. Handle missing values
#     print("\n" + "="*60)
#     print("STEP 4: Handling Missing Values")
#     print("="*60)
#     df = handle_missing_values(df)
    
#     # 6. Convert date format
#     print("\n" + "="*60)
#     print("STEP 5: Convert date format")
#     print("="*60)
#     df = convert_date_format(df)
    
#     # 6 Show final cleaned data
#     print("\n" + "="*60)
#     print("STEP 1: FINAL CLEANED DATA - First 5 Rows:")
#     print("="*60)
#     print(df.head(15))
    
#     print("\n" + "="*60)
#     print("COLUMN DATA TYPES")
#     print("="*60)
#     print(df.dtypes)
    
#     print("\n" + "="*60)
#     print("SUMMARY")
#     print("="*60)
#     print(f"✅ Total rows: {len(df)}")
#     print(f"✅ Total columns: {len(df.columns)}")
#     print(f"✅ Null values in data_date: {df['data_date'].isna().sum()} ")
    
#     # # INSPECT RAW DATA (Before any cleaning)
#     # pd.set_option('display.max_columns', None)
#     # pd.set_option('display.width', None)
#     # pd.set_option('display.max_colwidth', 50)
    
#     # print("\n" + "="*80)
#     # print("RAW DATA - FIRST 50 ROWS (NO CLEANING YET):")
#     # print("="*80)
#     # print(df.head(50))
    
#     # print("\n" + "="*80)
#     # print("RAW DATA - NULL VALUES PER COLUMN:")
#     # print("="*80)
#     # print(df.isnull().sum())
    
#     # # Display ALL columns without truncation
#     # pd.set_option('display.max_columns', None)
#     # pd.set_option('display.width', None)
#     # pd.set_option('display.max_colwidth', 50)
    
#     # print("\n" + "="*80)
#     # print("FIRST 5 ROWS WITH ALL COLUMNS (FULL INSPECTION):")
#     # print("="*80)
#     # print(df.head(50))
    
    
#     # Check for any remaining data quality issues
#     print("\n" + "="*80)
#     print("DATA QUALITY CHECK:")
#     print("="*80)
#     print(f"Total NULL values per column:")
#     print(df.isnull().sum())
    
#     print("\n" + "="*80)
#     print("UNIQUE VALUES IN KEY COLUMNS:")
#     print("="*80)
#     print(f"Unique statuses: {df['status'].unique()}")
#     print(f"Unique ownership types: {df['ownership_type'].unique()}")
#     print(f"Unique states: {df['state'].unique()}")
    
    
'''clean.py transforms messy CSV column names into clean 
database-friendly names, removes extra whitespace, and 
converts invalid date values to NULL.'''



'''Data cleaning has three parts. First, we rename columns 
from the CSV's messy format like 'data.date' to database-friendly 
names like 'data_date'. Second, we strip extra whitespace from 
text values to prevent duplicate entries. Third, we handle missing 
values by converting placeholder values like '0' in the date field 
to NULL, which is the proper way to represent missing data in a 
database. This ensures our data is consistent, accurate, and ready 
for validation.'''