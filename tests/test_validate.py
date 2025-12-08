# use pytest for testing

import pytest
import pandas as pd
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
from validate import validate_required_fields, validate_numeric_ranges, remove_duplicates

@pytest.fixture
def valid_sample_data():
    
    return pd.DataFrame({
        'location_id': ['CT001', 'CT002', 'CT003'],
        'city': ['Hartford', 'New Haven', 'Bridgeport'],
        'parking_spaces': [10, 0, 5],
        'state': ['CT', 'CT', 'CT']
    })
    
@pytest.fixture
def data_with_nulls():
    return pd.DataFrame({
        'location_id': ['CT001', None, 'CT003'],
        'city': ['Hartford', 'New Haven', None],
        'parking_spaces': [10, 0, 5],
        'state': ['CT', 'CT', 'CT']
    })
    
@pytest.fixture
def data_with_negative_parking():
    return pd.DataFrame({
        'location_id': ['CT001', 'CT002', 'CT003'],
        'city': ['Hartford', 'New Haven', 'Bridgeport'],
        'parking_spaces': [10, -5, 3],  # -5 is invalid
        'state': ['CT', 'CT', 'CT']
    })
    
@pytest.fixture
def data_with_duplicates():
    return pd.DataFrame({
        'location_id': ['CT001', 'CT002', 'CT001'],  # CT001 appears twice
        'city': ['Hartford', 'New Haven', 'Hartford'],
        'parking_spaces': [10, 0, 10],
        'state': ['CT', 'CT', 'CT']
    })
    
    
class TestValidRequiredFields:
    
    def test_all_valid_records(self, valid_sample_data):
        valid, rejected = validate_required_fields(valid_sample_data)
        
        assert len(valid) == 3
        assert len(rejected) == 0
        
    def test_missing_location_id(self, data_with_nulls):
        valid, rejected = validate_required_fields(data_with_nulls)
        
        assert len(valid) == 1
        assert len(rejected) == 2
        
    def test_messing_city(self, data_with_nulls):
        valid, rejected = validate_required_fields(data_with_nulls)
        
        assert len(rejected) >= 1
        rejected_cities = rejected['city'].tolist()
        assert any(pd.isna(c) or c is None for c in rejected_cities)
        
    def test_empty(self):
        empty_df = pd.DataFrame(columns=['location_id', 'city'])
        valid, rejected = validate_required_fields(empty_df)
        
        assert len(valid) == 0
        assert len(rejected) == 0
        
        
class TestValidateNumericRanges:
    
    def test_all_valid_parking_spaces(self, valid_sample_data):
        valid, rejected = validate_numeric_ranges(valid_sample_data)
        
        assert len(valid) == 3
        assert len(rejected) == 0
        assert all(valid['parking_spaces'] >= 0) 
        
    def test_negative_parkig_spaces(self, data_with_negative_parking):
        valid, rejected = validate_numeric_ranges(data_with_negative_parking)
        
        assert len(valid) == 2
        assert len(rejected) == 1
        assert rejected.iloc[0]['parking_spaces'] == -5
    
    def test_zero_parking_allowed(self, valid_sample_data):
        valid, rejects = validate_numeric_ranges(valid_sample_data)
        
        zero_parking = valid[valid['parking_spaces'] == 0]
        assert len(zero_parking) == 1
        
class TestRemoveDuplicates:
    
    def test_no_duplicates(self, valid_sample_data):
        result = remove_duplicates(valid_sample_data, primary_key='location_id')
        
        assert len(result) == 3
        assert result['location_id'].tolist() == ['CT001', 'CT002', 'CT003']
        
    def test_remove_duplicates_keeps_first(self, data_with_duplicates):
        result = remove_duplicates(data_with_duplicates, primary_key='location_id')
        
        assert len(result) == 2
        assert 'CT001' in result['location_id'].values
        assert 'CT002' in result['location_id'].values
        
        ct001_row = result[result['location_id'] == 'CT001'].iloc[0]
        assert ct001_row['city'] == 'Hartford'
        
    def test_multiples_duplicates(self):
        df = pd.DataFrame({
            'location_id': ['A', 'B', 'A', 'C', 'B', 'A'],
            'value': [1, 2, 3, 4, 5, 6]
        })
        result = remove_duplicates(df, primary_key='location_id')
        
        assert len(result) == 3
        assert set(result['location_id'].values) == {'A', 'B', 'C'}
        
        assert result[result['location_id'] == 'A'].iloc[0]['value'] == 1 
        assert result[result['location_id'] == 'B'].iloc[0]['value'] == 2 