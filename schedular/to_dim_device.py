from prefect import flow, task
from utility.execute_query import QueryExecutor


sql_query = open('sql/to_dim_device.sql').read()

query_executer = QueryExecutor()
conn = query_executer.create_cursor_connection()
curr = conn.cursor()

@task
def to_dim_device():
    curr.execute(sql_query)
    conn.commit()
    curr.close()
    conn.close()
    

@flow()
def to_dim_device_flow():
    to_dim_device()

if __name__ == "__main__":
    to_dim_device_flow.serve(
        name="to_dim_device",
        cron="20 0 * * *",
        tags=["dw", "dim_device", "dimension"],
        description="Upsert dim_device table of data warehouse"
    )