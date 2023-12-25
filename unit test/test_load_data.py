import unittest
from unittest.mock import MagicMock, patch
from ..schedular.utility.load_data import DataLoader 

class TestDataLoader(unittest.TestCase):
    def setUp(self):
        # Mock the data warehouse engine
        self.dw_engine = MagicMock()

        # Create a DataLoader instance with the mocked data warehouse engine
        self.data_loader = DataLoader(self.dw_engine)

    def tearDown(self):
        pass

    @patch('utility.execute_query.QueryExecutor')
    def test_load_data_successful(self, mock_query_executor):
        # Mock the QueryExecutor instance and its methods
        mock_executor_instance = MagicMock()
        mock_query_executor.return_value = mock_executor_instance
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_executor_instance.create_cursor_connection.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor

        # Mock successful data loading process
        mock_cursor.execute.side_effect = [None, None]  # Mock the SELECT and COMMIT queries

        # Mock the Pandas DataFrame
        mock_data = MagicMock()
        mock_data.to_sql.return_value = None

        # Call the method to be tested
        result = self.data_loader.load_data(mock_data)

        # Assertions
        mock_query_executor.assert_called_once()
        mock_executor_instance.create_cursor_connection.assert_called_once()
        mock_data.to_sql.assert_called_once_with("temp_table_bronze", schema='bronze', if_exists='append', index=False, con=self.dw_engine)
        mock_cursor.execute.assert_has_calls([
            MagicMock(return_value=None),  # Mock the SELECT query
            MagicMock(return_value=None)   # Mock the COMMIT query
        ])
        mock_connection.commit.assert_called_once()
        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_called_once()
        self.assertTrue(result)

    @patch('utility.execute_query.QueryExecutor')
    def test_load_data_unsuccessful(self, mock_query_executor):
        # Mock the QueryExecutor instance and its methods
        mock_executor_instance = MagicMock()
        mock_query_executor.return_value = mock_executor_instance
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_executor_instance.create_cursor_connection.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor

        # Mock an exception during data loading
        mock_cursor.execute.side_effect = Exception("Mocked exception")

        # Mock the Pandas DataFrame
        mock_data = MagicMock()
        mock_data.to_sql.return_value = None

        # Call the method to be tested
        result = self.data_loader.load_data(mock_data)

        # Assertions
        mock_query_executor.assert_called_once()
        mock_executor_instance.create_cursor_connection.assert_called_once()
        mock_data.to_sql.assert_called_once_with("temp_table_bronze", schema='bronze', if_exists='append', index=False, con=self.dw_engine)
        mock_cursor.execute.assert_called_once()
        mock_connection.commit.assert_not_called()
        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_called_once()
        self.assertFalse(result)

if __name__ == '__main__':
    unittest.main()
