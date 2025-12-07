# validate.py - Functions to validate and transform data
# You'll build this step by step!

import pandas as pd
import numpy as np

def validate_required_fields(df):
    valid_mask = pd.Series([True] * len(df), index=df.index)
    all_rejects = pd.DataFrame()
    
    # Rule 1: location_id must not be null
    if 'location_id' in df.column:
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
    
    print(f"✅ Required fields validation: {len(valid_df)} valid, {len(all_rejects)} rejected")
    
    return valid_df, all_rejects
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
def validate_numeric_ranges(df):
    pass

def split_valid_invalid(df, valid_mask, reason):
    pass