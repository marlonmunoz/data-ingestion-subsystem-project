# test_null_validation.py - Tests for NULL value validation
import pytest
import pandas as pd
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
from validate import validate_null_values


class TestValidateNullValues:
    
    def test_all_valid_no_nulls(self):
        """Test with no NULL values - all records should be valid"""
        df = pd.DataFrame({
            'location_id': ['LOC001', 'LOC002'],
            'ownership_type': ['Owned', 'Leased'],
            'property_type': ['Office', 'Warehouse'],
            'zip_code': ['12345', '67890'],
            'address_line1': ['123 Main St', '456 Oak Ave']
        })
        
        valid_df, rejected_df = validate_null_values(df)
        
        assert len(valid_df) == 2
        assert len(rejected_df) == 0
    
    def test_null_ownership_type(self):
        """Test rejection of NULL ownership_type"""
        df = pd.DataFrame({
            'location_id': ['LOC001', 'LOC002'],
            'ownership_type': ['Owned', None],  # LOC002 has NULL
            'property_type': ['Office', 'Warehouse'],
            'zip_code': ['12345', '67890'],
            'address_line1': ['123 Main St', '456 Oak Ave']
        })
        
        valid_df, rejected_df = validate_null_values(df)
        
        assert len(valid_df) == 1
        assert len(rejected_df) == 1
        assert rejected_df.iloc[0]['location_id'] == 'LOC002'
        assert 'ownership_type' in rejected_df.iloc[0]['rejection_reason']
    
    def test_null_property_type(self):
        """Test rejection of NULL property_type"""
        df = pd.DataFrame({
            'location_id': ['LOC001', 'LOC002'],
            'ownership_type': ['Owned', 'Leased'],
            'property_type': [None, 'Warehouse'],  # LOC001 has NULL
            'zip_code': ['12345', '67890'],
            'address_line1': ['123 Main St', '456 Oak Ave']
        })
        
        valid_df, rejected_df = validate_null_values(df)
        
        assert len(valid_df) == 1
        assert len(rejected_df) == 1
        assert rejected_df.iloc[0]['location_id'] == 'LOC001'
        assert 'property_type' in rejected_df.iloc[0]['rejection_reason']
    
    def test_null_zip_code(self):
        """Test rejection of NULL zip_code"""
        df = pd.DataFrame({
            'location_id': ['LOC001', 'LOC002'],
            'ownership_type': ['Owned', 'Leased'],
            'property_type': ['Office', 'Warehouse'],
            'zip_code': ['12345', None],  # LOC002 has NULL
            'address_line1': ['123 Main St', '456 Oak Ave']
        })
        
        valid_df, rejected_df = validate_null_values(df)
        
        assert len(valid_df) == 1
        assert len(rejected_df) == 1
        assert rejected_df.iloc[0]['location_id'] == 'LOC002'
        assert 'zip_code' in rejected_df.iloc[0]['rejection_reason']
    
    def test_null_address_line1(self):
        """Test rejection of NULL address_line1"""
        df = pd.DataFrame({
            'location_id': ['LOC001', 'LOC002'],
            'ownership_type': ['Owned', 'Leased'],
            'property_type': ['Office', 'Warehouse'],
            'zip_code': ['12345', '67890'],
            'address_line1': [None, '456 Oak Ave']  # LOC001 has NULL
        })
        
        valid_df, rejected_df = validate_null_values(df)
        
        assert len(valid_df) == 1
        assert len(rejected_df) == 1
        assert rejected_df.iloc[0]['location_id'] == 'LOC001'
        assert 'address_line1' in rejected_df.iloc[0]['rejection_reason']
    
    def test_multiple_null_values(self):
        """Test rejection of multiple records with NULL values"""
        df = pd.DataFrame({
            'location_id': ['LOC001', 'LOC002', 'LOC003'],
            'ownership_type': ['Owned', None, 'Leased'],
            'property_type': ['Office', 'Warehouse', None],
            'zip_code': [None, '67890', '11111'],
            'address_line1': ['123 Main St', '456 Oak Ave', '789 Pine Rd']
        })
        
        valid_df, rejected_df = validate_null_values(df)
        
        # LOC001 has NULL zip_code
        # LOC002 has NULL ownership_type
        # LOC003 has NULL property_type
        assert len(valid_df) == 0
        assert len(rejected_df) == 3
    
    def test_nulls_in_non_critical_columns_allowed(self):
        """Test that NULL values in non-critical columns are allowed"""
        df = pd.DataFrame({
            'location_id': ['LOC001', 'LOC002'],
            'ownership_type': ['Owned', 'Leased'],
            'property_type': ['Office', 'Warehouse'],
            'zip_code': ['12345', '67890'],
            'address_line1': ['123 Main St', '456 Oak Ave'],
            'parking_spaces': [10, None],  # NULL in non-critical column
            'status': [None, 'Active']  # NULL in non-critical column
        })
        
        valid_df, rejected_df = validate_null_values(df)
        
        # Both records should pass because NULL values are in non-critical columns
        assert len(valid_df) == 2
        assert len(rejected_df) == 0
    
    def test_empty_dataframe(self):
        """Test with empty DataFrame"""
        df = pd.DataFrame({
            'location_id': [],
            'ownership_type': [],
            'property_type': [],
            'zip_code': [],
            'address_line1': []
        })
        
        valid_df, rejected_df = validate_null_values(df)
        
        assert len(valid_df) == 0
        assert len(rejected_df) == 0
