from prefect import flow, task
from utility.execute_query import QueryExecutor

sql_query = open('sql/to_fct_sales.sql').read()

query_executer = QueryExecutor()
conn = query_executer.create_cursor_connection()
curr = conn.cursor()

@task
def to_fct_sales():
    curr.execute(sql_query)
    conn.commit()
    curr.close()
    conn.close()
    

@flow()
def to_fct_sales_flow():
    to_fct_sales()

if __name__ == "__main__":
    to_fct_sales_flow.serve(
        name="to_fct_sales",
        cron="35 0 * * *",
        tags=["dw", "to_fct_sales", "fact"],
        description="Upsert fct_sales table of data warehouse"
    )