from prefect import flow, task
from utility.execute_query import QueryExecutor

sql_query = open('sql/to_dim_calendar.sql').read()

query_executer = QueryExecutor()
conn = query_executer.create_cursor_connection()
curr = conn.cursor()

@task
def to_dim_calendar():
    curr.execute(sql_query)
    conn.commit()
    curr.close()
    conn.close()
    

@flow()
def to_dim_calendar_flow():
    to_dim_calendar()

if __name__ == "__main__":
    to_dim_calendar_flow.serve(
        name="to_dim_calendar",
        tags=["dw", "to_dim_calendar", "dimension"],
        description="Fill to_dim_calendar table of data warehouse"
    )