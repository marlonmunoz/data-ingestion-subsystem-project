import pytest
import pandas as pd
import sys
import os 


sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
from clean import rename_columns, strip_whitespace, handle_missing_values, convert_date_format

class TestRenameColumns:
    
    def test_rename_columns_basic(self):
        df = pd.DataFrame({
            'data.date': ['2024-01-01'],
            'data.owned or leased': ['Owned'],
            'location.id': ['LOC001'],
            'location.address.city': ['Washington']
        })
        
        result = rename_columns(df)
        
        # new
        assert 'data_date' in result.columns
        assert 'ownership_type' in result.columns
        assert 'location_id' in result.columns
        assert 'city' in result.columns
        
        # olf
        assert 'data.date' not in result.columns
        assert 'location.id' not in result.columns
        
class TestStripWhitespace:
    
    def test_strip_whitespace_removes_space(self):
        df = pd.DataFrame({
            'city': ['  New York  ', 'Boston ', '  LA'],
            'state': ['NY  ', '  MA', 'CA'],
            'parking_spaces': [5, 10, 15]
        })
        
        result = strip_whitespace(df)
        
        assert result['city'].tolist() == ['New York', 'Boston', 'LA']
        assert result['state'].tolist() == ['NY', 'MA', 'CA']
        
        assert result['parking_spaces'].tolist() == [5,10,15]
        
    def test_strip_whitespace_empty_dataframe(self):
        df = pd.DataFrame()
        result = strip_whitespace(df)
        assert len(result) == 0
        
class TestHandleMissingValues:
    
    def test_handle_missing_values_converts_zero_string(self):
        df = pd.DataFrame({
            'data_date': ['2024-01-01', '0', '2024-03-01', '0'],
            'city': ['NYC', 'Boston', 'LA', 'SF'] 
        }) 
        
        result = handle_missing_values(df)
        
        assert result['data_date'].tolist() == ['2024-01-01', None, '2024-03-01', None]
        assert result['city'].tolist() == ['NYC', 'Boston', 'LA', 'SF']
    
    def test_handle_missing_values_no_data_date_column(self):
        df = pd.DataFrame({'city': ['NYC']})
        result = handle_missing_values(df)
        assert 'data_date' not in result.columns

class TestConvertDataFormat:
    def test_convert_date_format_valid_dates(self):
        df = pd.DataFrame({
            'data_date': ['2024-01-01', '2024-12-31', '2023-06-15']
        })
        
        result = convert_date_format(df)
        
        assert pd.api.types.is_datetime64_any_dtype(result['data_date'])
        
        assert result['data_date'].iloc[0] == pd.Timestamp('2024-01-01')
        assert result['data_date'].iloc[1] == pd.Timestamp('2024-12-31')

    def test_convert_date_format_invalid_dates(self):
        df = pd.DataFrame({
            'data_date': ['2024-01-01', 'invalid', None, '2024-99-99']            
        })
        
        result = convert_date_format(df)
        
        assert result['data_date'].iloc[0] == pd.Timestamp('2024-01-01')
        
        assert pd.isna(result['data_date'].iloc[1])
        assert pd.isna(result['data_date'].iloc[2])
        assert pd.isna(result['data_date'].iloc[3])
        
    def test_covert_date_format_no_column(self):
        df = pd.DataFrame({'city': ['NYC']})
        result = convert_date_format(df)
        assert 'data_date' not in result.columns
                
                
                