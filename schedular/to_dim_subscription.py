from prefect import flow, task
from utility.execute_query import QueryExecutor

sql_query = open('sql/to_dim_subscription.sql').read()

query_executer = QueryExecutor()
conn = query_executer.create_cursor_connection()
curr = conn.cursor()

@task
def to_dim_subscription():
    curr.execute(sql_query)
    conn.commit()
    curr.close()
    conn.close()
    

@flow()
def to_dim_subscription_flow():
    to_dim_subscription()

if __name__ == "__main__":
    to_dim_subscription_flow.serve(
        name="to_dim_subscription",
        cron="25 0 * * *",
        tags=["dw", "dim_subscription", "dimension"],
        description="Upsert dim_subscription table of data warehouse"
    )