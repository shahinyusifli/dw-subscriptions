import unittest
from unittest.mock import MagicMock, patch
from ..schedular.utility.execute_query import QueryExecutor 

class TestQueryExecutor(unittest.TestCase):
    def setUp(self):
        # Create a QueryExecutor instance
        self.query_executor = QueryExecutor()

    def tearDown(self):
        pass

    @patch('psycopg2.connect')
    @patch('prefect.blocks.system.JSON.load')
    def test_create_cursor_connection(self, mock_json_load, mock_psycopg2_connect):
        # Mock JSON.load to return a dictionary with connection parameters
        mock_json_load.return_value = {
            "local_host": "localhost",
            "port": 5432,
            "user_name": "test_user",
            "password": "test_password",
            "database": "test_database"
        }

        # Mock psycopg2.connect
        mock_connection = MagicMock()
        mock_psycopg2_connect.return_value = mock_connection

        # Call the method to be tested
        connection = self.query_executor.create_cursor_connection()

        # Assertions
        mock_json_load.assert_called_once_with("raw-connection-dw")
        mock_psycopg2_connect.assert_called_once_with(
            host="localhost",
            port=5432,
            user="test_user",
            password="test_password",
            database="test_database"
        )
        self.assertEqual(connection, mock_connection)

    def test_commit(self):
        # Test if the commit method does not raise an exception
        try:
            self.query_executor.commit()
        except Exception as e:
            self.fail(f"Unexpected exception: {e}")

    def test_close(self):
        # Test if the close method does not raise an exception
        try:
            self.query_executor.close()
        except Exception as e:
            self.fail(f"Unexpected exception: {e}")

if __name__ == '__main__':
    unittest.main()
