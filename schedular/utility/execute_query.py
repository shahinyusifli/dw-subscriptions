import psycopg2
from prefect.blocks.system import JSON

class QueryExecutor:
    def __init__(self):
        # Parse JSON string to extract connection information from Prefect's JSON block
        row_dw_credentials = JSON.load("raw-connection-dw")
        
        # Extract connection parameters
        self.host = row_dw_credentials.value["local_host"]
        self.port = row_dw_credentials.value["port"]
        self.user = row_dw_credentials.value["user_name"]
        self.password = row_dw_credentials.value["password"]
        self.database = row_dw_credentials.value["database"]

    def create_cursor_connection(self):
        """
        Creates a connection to the database using the extracted connection parameters.

        :return: psycopg2 connection object.
        """
        connection = psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database
        )
        return connection

    def commit(self):
        """
        Commits changes to the database.
        
        Note: You can implement this method if needed for transactional operations.
        """
        pass  # You can implement this method if needed

    def close(self):
        """
        Closes the database connection.
        
        Note: You can implement this method if needed to explicitly close the connection.
        """
        pass  # You can implement this method if needed
