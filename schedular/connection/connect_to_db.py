from sqlalchemy import create_engine

class DatabaseConnector:
    def __init__(self, db_url, echo=False):
        """
        Initializes the DatabaseConnector with the given database URL and an optional flag for echoing SQL statements.
        
        :param db_url: The URL for the database connection.
        :param echo: Boolean flag to enable or disable echoing SQL statements (default is False).
        """
        self.db_url = db_url
        self.echo = echo
        self.engine = self.create_engine()

    def create_engine(self):
        """
        Creates and returns a SQLAlchemy engine using the specified database URL and echo setting.
        
        :return: SQLAlchemy engine instance.
        """
        return create_engine(self.db_url, echo=self.echo)

    def get_connection(self):
        """
        Establishes a database connection and returns a connection object.
        
        :return: Database connection object.
        """
        try:
            connection = self.engine.connect()
            return connection
        except Exception as e:
            print(f"Error connecting to the database: {e}")
            return False

    def close_connection(self, connection):
        """
        Closes the provided database connection.
        
        :param connection: Database connection object to be closed.
        """
        connection.close()
