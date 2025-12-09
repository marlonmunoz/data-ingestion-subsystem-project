# use pytest for testing
# mock

import pytest
import pandas as pd
import sys
import os
from unittest.mock import Mock, MagicMock, patch, call

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from load import get_db_connection, create_tables, load_to_staging, load_rejected


# My Fixtures
@pytest.fixture
def sample_valid_data():
    return pd.DataFrame({
        'location_id': ['CT001', 'CT002', 'CT003'],
        'city': ['Hartford', 'New Haven', 'Bridgeport'],
        'parking_spaces': [10, 0, 5],
        'state': ['CT', 'CT', 'CT']
    })

@pytest.fixture
def sample_rejected_data():
    return pd.DataFrame({
        'location_id': ['CT999'],
        'city': [None],
        'rejection_reason': ['Missing city']
    })
    
    
@pytest.fixture
def mock_db_connection():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn, mock_cursor


# Testing my get_db_connection

class TestGetDbConnetion:
    
    @patch('load.psycopg2.connect')
    def test_successful_connection(self, mock_connect):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        db_url = "postgresql://user:pass@localhost:5432/testdb"
        conn = get_db_connection(db_url)
        
        mock_connect.assert_called_once_with(db_url)
        assert conn == mock_conn
        
    @patch('load.psycopg2.connect')
    def test_connection_failure(self, mock_connection):
        mock_connection.side_effect = Exception("Connection failed")
        
        db_url = "postgresql://user:pass@localhost:5432/testdb"
        
        with pytest.raises(Exception) as exc_info:
            get_db_connection(db_url)
            
        assert "Connection failed" in str(exc_info.value)
        

# Test create_tables
class TestCreateTables:
    
    def test_create_tables_executes_ddl(self, mock_db_connection):
        mock_conn, mock_cursor = mock_db_connection
        
        create_tables(mock_conn)
        
        assert mock_cursor.execute.called
        assert mock_cursor.execute.call_count >= 2
        
        mock_conn.commit.assert_called_once()
        
    def test_create_tables_closes_cursor(self, mock_db_connection):
        
        mock_conn, mock_cursor = mock_db_connection
        
        create_tables(mock_conn)
        
        mock_cursor.close.assert_called_once()
        
        
# Test load_to_staging
class TestLoadToStaging:
    
    def test_load_valid_data(slef, mock_db_connection, sample_valid_data):
        
        mock_conn, mock_cursor = mock_db_connection
        
        rows_loaded = load_to_staging(
            mock_conn,
            sample_valid_data,
            table_name='stg_test',
            pk_column='location_id',
            batch_size=2
        )
        
        assert rows_loaded == 3
        
        assert mock_cursor.execute.call_count == 3
        
        assert mock_conn.commit.called
        
    def test_load_with_values(self, mock_db_connection):
        mock_conn, mock_cursor = mock_db_connection
        
        df_with_nulls = pd.DataFrame({
            'location_id': ['CT001', 'CT002'],
            'city': ['Hartford', None],  # NULL value
            'parking_spaces': [10, 0]
        })
        
        rows_loaded = load_to_staging(
            mock_conn,
            df_with_nulls,
            table_name='stg_table',
            pk_column='location_id',
            batch_size=10
        )
        
        assert rows_loaded == 2
        assert mock_cursor.execute.call_count == 2
        
    def test_load_empty_dataframe(self, mock_db_connection):
        mock_conn, mock_cursor = mock_db_connection
        
        empty_df = pd.DataFrame(columns=['location_id', 'city'])
        
        rows_loaded = load_to_staging(
            mock_conn,
            empty_df,
            table_name='stg_test',
            pk_column='location_id',
            batch_size=10
        )
        
        assert rows_loaded == 0
        
        mock_cursor.execute.assert_not_called()
        
    def test_batch_processsing(self, mock_db_connection, sample_valid_data):
        mock_conn, mock_cursor = mock_db_connection
        
        load_to_staging(
            mock_conn,
            sample_valid_data,
            table_name='stg_test',
            pk_column='location_id',
            batch_size=1
        )
        assert mock_conn.commit.call_count == 3
        
    def test_upsert_query_structure(self, mock_db_connection, sample_valid_data):
        mock_conn, mock_cursor = mock_db_connection
        
        load_to_staging(
            mock_conn,
            sample_valid_data,
            table_name='stg_test',
            pk_column='location_id',
            batch_size=10
        )
        
        first_call_args = mock_cursor.execute.call_args_list[0][0]
        sql_query = first_call_args[0]
        
        assert 'INSERT INTO stg_test' in sql_query
        assert 'VALUES' in sql_query
        assert 'ON CONFLICT (location_id)' in sql_query
        assert  'DO UPDATE SET' in sql_query
        

# Test load_rejected
class TestLoadRejected:
    
    def test_load_rejected_records(self, mock_db_connection, sample_rejected_data):
        mock_conn, mock_cursor = mock_db_connection
        
        rows_loaded = load_rejected(
            mock_conn,
            sample_rejected_data,
            source_name='test_source'
        )
        
        assert rows_loaded == 1
        mock_cursor.execute.assert_called_once()
        mock_conn.commit.assert_called_once()
        
    
    def test_load_empty_rejected_dataframe(self, mock_db_connection):
        mock_conn, mock_cursor = mock_db_connection
        
        empty_df = pd.DataFrame()
        
        rows_loaded = load_rejected(
            mock_conn,
            empty_df,
            source_name='test_source'
        )
        
        assert rows_loaded == 0
        
        mock_cursor.execute.assert_not_called()
        
    def test_rejected_json_structure(self, mock_db_connection, sample_rejected_data):
        mock_conn, mock_cursor = mock_db_connection
        
        load_rejected(
            mock_conn,
            sample_rejected_data,
            source_name='test_source'
        )
        
        call_args = mock_cursor.execute.call_args_list[0][0]
        sql_query = call_args[0]
        
        # Verify INSERT structure
        assert 'INSERT INTO stg_rejects' in sql_query
        assert 'source_name' in sql_query
        assert 'raw_data' in sql_query
        assert 'rejection_reason' in sql_query
        

# Integration test
class TestDatabaseIntegration:
    def test_full_loaded_workflow(slef, mock_db_connection, sample_valid_data):
        mock_conn, mock_cursor = mock_db_connection
        
        create_tables(mock_conn)
        
        rows_loaded = load_to_staging(
            mock_conn,
            sample_valid_data,
            table_name='stg_real_estate',
            pk_column='location_id',
            batch_size=1000
        )
        
        assert rows_loaded == 3
        assert mock_conn.commit.call_count >= 2

class TestErrorHandling:
    
    def test_get_db_connection_invalid_url(self):
        invalid_db_url = "postgresql://baduser:badpass@localhost:9999/nonexistent"
        
        with pytest.raises(Exception):
            get_db_connection(invalid_db_url)
            
    def test_create_table_rollback_on_error(self, mock_db_connection):
        
        conn, cursor = mock_db_connection
        
        cursor.execute.side_effect = Exception("SQL Error")
        
        with pytest.raises(Exception):
            create_tables(conn)
            
        conn.rollback.assert_called_once()
        cursor.close.assert_called_once()
        
    def test_load_to_staging_rollback_on_error(self, mock_db_connection, sample_valid_data):
        conn, cursor = mock_db_connection
        cursor.execute.side_effect = Exception("Insert failed")
        
        with pytest.raises(Exception):
            load_to_staging(conn, sample_valid_data, 'stg_real_estate', 'location_id')
            
            conn.rollback.assert_called()
            cursor.close.assert_called()
            
                       
    def test_load_rejected_rollback_or_error(self, mock_db_connection, sample_rejected_data):
        
        conn, cursor = mock_db_connection
        cursor.execute.side_effect = Exception("Insert rejected failed")
        
        with pytest.raises(Exception):
            load_rejected(conn, sample_rejected_data, 'test_source')
            
            conn.rollback.assert_called()
            cursor.close.assert_called()
            
        
    