import pytest
import pandas as pd
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
from rules import apply_all_validations

class TestApplyAllValidations:
    def test_all_valid_reports(self):
        df = pd.DataFrame({
            'location_id': ['LOC001', 'LOC002', 'LOC003'],
            'city': ['Hartford', 'Boston', 'New York'],
            'state': ['CT', 'MA', 'NY'],
            'parking_spaces': [10, 5, 0] 
        })
        
        valid_df, rejected_df = apply_all_validations(df, primary_key='location_id')
        
        
        assert len(valid_df) == 3
        assert len(rejected_df) == 0
        
    def test_missing_required_fields(self):
        df = pd.DataFrame({
            'location_id': ['LOC001', None, 'LOC003'],
            'city': ['Hartford', 'Boston', None],
            'state': ['CT', 'MA', 'NY'],
            'parking_spaces': [10, 5, 8]         
        })
        
        valid_df, rejected_df = apply_all_validations(df)
        
        assert len(valid_df) == 1
        assert len(rejected_df) == 2
        assert valid_df.iloc[0]['location_id'] == 'LOC001'
        
    def test_negative_parking_spaces(self):
        df = pd.DataFrame({
            'location_id': ['LOC001', 'LOC002', 'LOC003'],
            'city': ['Hartford', 'Boston', 'New York'],
            'state': ['CT', 'MA', 'NY'],
            'parking_spaces': [10, -5, 8]
        })
        
        valid_df, rejected_df = apply_all_validations(df)
        
        assert len(valid_df) == 2
        assert len(rejected_df) == 1
        assert rejected_df.iloc[0]['location_id'] == 'LOC002'
        assert 'parking_spaces' in rejected_df.iloc[0]['rejection_reason'].lower()
        
    def test_duplacate_removal(self):
        df = pd.DataFrame({
            'location_id': ['LOC001', 'LOC002', 'LOC001'],
            'city': ['Hartford', 'Boston', 'Hartford'],
            'state': ['CT', 'MA', 'CT'],
            'parking_spaces': [10, 5, 10]
        })
        
        valid_df, rejected_df = apply_all_validations(df)
        
        assert len(valid_df) == 2
        assert valid_df['location_id'].tolist() == ['LOC001', 'LOC002']
        
    def test_multiple_validation_failures(self):
        df = pd.DataFrame({
            'location_id': ['LOC001', None, 'LOC003', 'LOC004'],
            'city': ['Hartford', 'Boston', '', 'LA'],
            'state': ['CT', 'MA', 'NY', 'CA'],
            'parking_spaces': [10, 5, 8, -3]
        })
        
        valid_df, rejected_df = apply_all_validations(df)
        
        assert len(valid_df) == 1
        assert len(rejected_df) == 3
        assert valid_df.iloc[0]['location_id'] == 'LOC001'
        
    def test_empty_dataframe(self):
        df = pd.DataFrame(columns=['location_id', 'city', 'state', 'parking_spaces'])
        
        valid_df, rejected_df = apply_all_validations(df)
        
        assert len(valid_df) == 0
        assert len(rejected_df) == 0
        
    def test_rejection_reasons_tracked(self):
        df = pd.DataFrame({
            'location_id': [None, 'LOC002'],
            'city': ['Hartford', 'Boston'],
            'state': ['CT', 'MA'],
            'parking_spaces': [10, -5]
        })
        
        valid_df, rejected_df = apply_all_validations(df)
        
        assert len(rejected_df) == 2
        reasons = rejected_df['rejection_reason'].tolist()
        assert 'location_id' in reasons[0].lower()
        assert 'parking' in reasons[1].lower()