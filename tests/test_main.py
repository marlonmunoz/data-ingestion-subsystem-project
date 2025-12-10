import pytest
import sys
import os
from unittest.mock import Mock, patch, MagicMock
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

class TestRunPipiLines

@patch('main.load_rejected')
@patch('main.load_to_staging')
@patch('main.create_tables')
@patch('main.get_db_conneection')
@patch('main.apply_all_validations')
@patch('main.convert_date_format')
@patch('main.handle_missing_values')
@patch('main.strip_whitespace')
@patch('main.rename_columns')
@patch('main.read_csv')
@patch('main.load_config')
def test_successful_pipeline_execution(
    self,
    mock_load_config,
    mock_read_csv,
    mock_rename_columns,
    mock_strip_whitespace,
    mock_handle_missing_values,
    mock_convert_date_format,
    mock_apply_all_validations,
    mock_get_db_connection,
    mock_create_tables,
    mock_load_to_staging,
    mock_load_rejected
):
    mock_load_config.return_value = {
        'sources': [{
                'name': 'test_source',
                'path': 'test.csv',
                'target_table': 'test_table',
                'primary_key': 'id'
        }],
        'defaults': {
            'db_url': 'postgresql://test:test@localhost/testdb',
            'batch_size': 100
        }
    }
    
    test_df = pd.DataFrame({
            'id': ['1', '2', '3'],
            'name': ['A', 'B', 'C']
    })
    
    valid_df = pd.DataFrame({
        'id': ['1', '2'],
        'name': ['A', 'B']
    })
    
    rejected_df = pd.DataFrame({
        'id': ['3'],
        'name': ['C'],
        'rejection_reason': ['Invalid']
    })
    
    mock_read_csv.return_value = test_df
    mock_rename_columns.return_value = test_df
    mock_strip_whitespace.return_value = test_df
    
        
        
    