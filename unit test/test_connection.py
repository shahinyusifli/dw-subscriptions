import unittest
from unittest.mock import MagicMock
from ..schedular.connection.connect_to_db import DatabaseConnector 

class TestDatabaseConnector(unittest.TestCase):
    def setUp(self):
        # Create a DatabaseConnector instance with a dummy database URL for testing
        self.connector = DatabaseConnector("sqlite:///:memory:")

    def tearDown(self):
        pass

    def test_create_engine(self):
        # Test if create_engine method returns a SQLAlchemy engine
        engine = self.connector.create_engine()
        self.assertIsNotNone(engine)
        self.assertTrue(hasattr(engine, 'connect'))

    def test_get_connection(self):
        # Mock the SQLAlchemy engine's connect method to avoid actual database connection
        self.connector.engine.connect = MagicMock(return_value="mocked_connection")

        # Test if get_connection method returns a connection object
        connection = self.connector.get_connection()
        self.assertEqual(connection, "mocked_connection")

    def test_close_connection(self):
        # Mock a database connection object
        mock_connection = MagicMock()

        # Test if close_connection method closes the provided connection
        self.connector.close_connection(mock_connection)
        mock_connection.close.assert_called_once()

if __name__ == '__main__':
    unittest.main()
