from prefect import flow, task
from utility.execute_query import QueryExecutor

sql_query = open('sql/to_dim_account.sql').read()

query_executer = QueryExecutor()
conn = query_executer.create_cursor_connection()
curr = conn.cursor()

@task
def to_dim_account():
    curr.execute(sql_query)
    conn.commit()
    curr.close()
    conn.close()        
    

@flow()
def to_dim_account_flow():
    to_dim_account()

if __name__ == "__main__":
    to_dim_account_flow.serve(
        name="to_dim_account",
        cron="30 0 * * *",
        tags=["dw", "to_dim_account", "dimension"],
        description="Upsert to_dim_account table of data warehouse"
    )