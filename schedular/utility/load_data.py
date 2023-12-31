from utility.execute_query import QueryExecutor

class DataLoader:
    def __init__(self, dw_engine):
        """
        Initializes the DataLoader with a data warehouse engine.

        Args:
            dw_engine: An object providing an engine for the data warehouse.
        """
        self.dw_engine = dw_engine

    def load_data(self, data):
        """
        Loads data into the data warehouse by executing SQL queries.

        Args:
            data (pd.DataFrame): A Pandas DataFrame containing the data to be loaded.

        Returns:
            bool: True if the data loading is successful, False otherwise.
        """
        # Initialize a QueryExecutor to manage database queries
        query_executer = QueryExecutor()
        # Create a connection and cursor for executing SQL queries
        conn = query_executer.create_cursor_connection()
        curr = conn.cursor()

        try:
            # Load data into a temporary table in the 'bronze' schema
            data.to_sql("temp_table_bronze", schema='bronze', if_exists='append', index=False, con=self.dw_engine)

            # Execute a SQL query to merge the temporary table into the 'subscriptions' table
            curr.execute('SELECT bronze.merge_temp_table_into_subscriptions();')

            # Commit changes to the database and close the cursor and connection
            conn.commit()
            curr.close()
            conn.close()

            return True  # Data loading successful
        except Exception as e:
            # Print an error message if an exception occurs during data loading
            print(f"Error during data loading: {str(e)}")
            return False  # Data loading unsuccessful
