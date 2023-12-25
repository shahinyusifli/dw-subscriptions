from prefect import flow, task
from prefect.blocks.system import JSON
from connection.connect_to_db import DatabaseConnector
from utility.extract_data import DataExtractor
from utility.load_data import DataLoader

# Load credentials and SQL query from Prefect JSON blocks and files
sqlite_credential = JSON.load("sqlite")
dw_credential = JSON.load("dw-postgres")
sql_query = open('sql/to_bronze.sql').read()

# Extract SQLite credentials and create a connection to SQLite database
db_path = sqlite_credential.value["url"]
db_url = f"sqlite:///{db_path}"
db_connector = DatabaseConnector(db_url)
db_engine = db_connector.create_engine()

# Extract Data Warehouse (DW) credentials and create a connection to DW database (PostgreSQL)
dw_url = dw_credential.value["url"]
dw_connector = DatabaseConnector(dw_url)
dw_engine = dw_connector.create_engine()

# Initialize DataExtractor and DataLoader instances
data_extractor = DataExtractor(db_engine)
data_loader = DataLoader(dw_engine)

@task
def to_bronze():
    """
    Prefect task to extract data from an OLTP database and load it into the bronze stage of the data warehouse.
    """
    result_dataframe = data_extractor.extract_data(sql_query)
    data_loader.load_data(result_dataframe)

@flow()
def to_bronze_flow():
    """
    Prefect flow to define the data pipeline for extracting and loading data into the bronze stage.
    """
    to_bronze()

if __name__ == "__main__":
    # Serve the Prefect flow with scheduling and metadata
    to_bronze_flow.serve(
        name="to_bronze",
        cron="15 0 * * *",  # Schedule the flow to run daily at midnight
        tags=["dw", "bronze", "raw"],
        description="Select data from OLTP database to bronze stage of data warehouse"
    )
