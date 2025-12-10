import pytest
import sys
import os
from unittest.mock import Mock, patch, MagicMock
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

class TestRunPipiLines:

    @patch('main.load_rejected')
    @patch('main.load_to_staging')
    @patch('main.create_tables')
    @patch('main.get_db_connection')
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
        mock_handle_missing_values.return_value = test_df
        mock_convert_date_format.return_value = test_df
        mock_apply_all_validations.return_value = (valid_df, rejected_df)

        mock_conn = Mock()
        mock_get_db_connection.return_value = mock_conn
        mock_load_to_staging.return_value = 2
        mock_load_rejected.return_value = 1

        from main import run_pipeline
        valid_count,  rejected_count = run_pipeline('config/sources.json')

        mock_load_config.assert_called_once_with('config/sources.json')
        mock_read_csv.assert_called_once_with('test.csv')
        mock_rename_columns.assert_called_once()
        mock_strip_whitespace.assert_called_once()
        mock_handle_missing_values.assert_called_once()
        mock_convert_date_format.assert_called_once()
        mock_apply_all_validations.assert_called_once()
        mock_get_db_connection.assert_called_once()
        mock_create_tables.assert_called_once_with(mock_conn)
        mock_load_to_staging.assert_called_once()
        mock_load_rejected.assert_called_once()
        mock_conn.close.assert_called_once()

        assert valid_count == 2
        assert rejected_count == 1

    @patch('main.load_config')
    def test_pipeline_failure_on_config_load(self, mock_load_config):
        mock_load_config.side_effect = FileNotFoundError("Config not found")

        from main import run_pipeline
        with pytest.raises(SystemExit) as exc_info:
            run_pipeline('invalid.json')

        assert exc_info.value.code == 1

    @patch('main.get_db_connection')
    @patch('main.apply_all_validations')
    @patch('main.convert_date_format')
    @patch('main.handle_missing_values')
    @patch('main.strip_whitespace')
    @patch('main.rename_columns')
    @patch('main.read_csv')
    @patch('main.load_config')
    def test_pipeline_failure_on_database_connection(
        self,
        mock_load_config,
        mock_read_csv,
        mock_renamea_columns,
        mock_strip_whitespace,
        mock_handle_missing_values,
        mock_convert_date_format,
        mock_apply_all_validation,
        mock_get_db_connection
    ):

        mock_load_config.return_value = {
            'sources': [{
                    'name': 'test',
                    'path': 'test.csv',
                    'target_table': 'test_table'
            }],
            'defaults': {
                'db_url': 'bad_url',
                'batch_size': 100
            }
        }
        
        test_df = pd.DataFrame({'id': ['1']})
        mock_read_csv.return_value = test_df
        mock_renamea_columns.return_value = test_df
        mock_strip_whitespace.return_value = test_df
        mock_handle_missing_values.return_value = test_df
        mock_convert_date_format.return_value = test_df
        mock_apply_all_validation.return_value = (test_df, pd.DataFrame())
        
        mock_get_db_connection.side_effect = Exception("Connection failed")
        
        from main import run_pipeline
        with pytest.raises(SystemExit):
            run_pipeline()
            
    @patch('main.load_rejected')
    @patch('main.load_to_staging')
    @patch('main.create_tables')
    @patch('main.get_db_connection')
    @patch('main.apply_all_validations')
    @patch('main.convert_date_format')
    @patch('main.handle_missing_values')
    @patch('main.strip_whitespace')
    @patch('main.rename_columns')
    @patch('main.read_csv')
    @patch('main.load_config')
    def test_database_connection_closed_on_success(
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
            'sources': [{'name': 'test', 'path': 'test.csv', 'target_table': 'test'}],
            'defaults': {'db_url': 'test', 'batch_size': 100}
        }
        
        test_df = pd.DataFrame({'id': ['1']})
        mock_read_csv.return_value = test_df
        mock_rename_columns.return_value = test_df
        mock_strip_whitespace.return_value = test_df
        mock_handle_missing_values.return_value = test_df
        mock_convert_date_format.return_value = test_df
        mock_apply_all_validations.return_value = (test_df, pd.DataFrame())
        
        mock_conn = Mock()
        mock_get_db_connection.return_value = mock_conn
        mock_load_to_staging.return_value = 1
        mock_load_rejected.return_value = 0
        
        from main import run_pipeline
        run_pipeline()
        
        mock_conn.close.assert_called_once()
        
    
    
    
    
    
    
    
        
        
    