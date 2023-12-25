
from prefect import flow, task
from prefect.blocks.system import JSON
from connection.connect_to_db import DatabaseConnector
from utility.generate_data import  DataGenerator

sqlite_credential = JSON.load("sqlite")
db_path = sqlite_credential.value["url"]
db_url = f"sqlite:///{db_path}"
connector = DatabaseConnector(db_url)
data_generator = DataGenerator()

@task
def to_oltp_database():
    generated_data = data_generator.generate_data()
    engine = connector.create_engine()
    generated_data.to_sql('subscriptions', engine, if_exists='append', index=False)

@flow()
def to_oltp_database_flow():
    to_oltp_database()

if __name__ == "__main__":
    to_oltp_database_flow.serve(
        name="to_oltp_database",
        cron="0 0 * * *",
        tags=["otlp", "data_generate", "sqlite"],
        description="Generate 100 rows and insert them to subscriptions table of subscription database"
    )