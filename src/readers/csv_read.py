# csv_read.py - Functions to read data from different sources
# You'll build this step by step!

import pandas as pd
import sys 
import os


def read_csv(file_path): #LOAD CSV
    # read CSV file into a pandas DataFrame.
    df = pd.read_csv(file_path)
    
    print("\n" + "="*50)
    print(f"Loaded {len(df)} rows from {file_path}") #COUNT ROWS
    print("="*50)
    print("\n" + "="*50)
    print(f"Columns: {list(df.columns)}") #GET COLUMNS NAMES
    print("="*50)
    print("\n" + "="*50)
    print(f"Shape: {df.shape}") #(ROES, COLUMNS) TUPLE
    print("="*50)
    return df

if __name__=="__main__":
    # Test the function
    df = read_csv("data/real_estate.csv")
    print("\n" + "="*50)
    print("FIRST 3 ROWS:")
    print("="*50)
    print(df.head(3)) # Shows the first 3 rows
    print("\n" + "="*50)
    print("NUMBER OF ROWS")
    print("="*50)
    print(len(df)) # Number of rows
    
    print("\n" + "="*50)
    print("DATA TYPES:")
    print("="*50)
    print(df.dtypes) # Shows data type of each columns
    
    print("\n" + "="*50)
    print("BASIC INFO:")
    print("="*50)
    print(df.info()) # Summary: columns, types, nulls
    
    
'''   Data Exploration Pattern

    1. Load data               ✅
    2. Check size/shape        ✅
    3. Preview first few rows  ✅
    4. Examine data types      ✅
    5. Look for missing values ✅'''
    
    
''' The CSV reader uses pandas' read_csv() function to load the 
real estate data into a DataFrame. It reads 9,130 property 
records with 15 columns. The function prints diagnostic 
information showing the data shape, column names, and data 
types, which helps us understand what we're working with 
before we start cleaning and validating '''


'''csv_read.py reads the CSV file into a pandas DataFrame 
so we can manipulate the data in memory.'''