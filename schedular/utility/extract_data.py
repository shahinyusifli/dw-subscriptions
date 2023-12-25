import pandas as pd

class DataExtractor:
    def __init__(self, connector):
        """
        Initializes the DataExtractor with a database connector.

        Args:
            connector: An object providing a connection to the database.
        """
        self.connector = connector

    def extract_data(self, query):
        """
        Extracts data from the database using the provided SQL query.

        Args:
            query (str): SQL query to extract data from the database.

        Returns:
            pd.DataFrame or None: A Pandas DataFrame containing the extracted data, or None if an error occurs.
        """
        try:
            # Use Pandas to execute the SQL query and retrieve data into a DataFrame
            data = pd.read_sql(query, con=self.connector)
            return data
        except Exception as e:
            # Print an error message if an exception occurs during data extraction
            print(f"Error during data extraction: {str(e)}")
            return None
