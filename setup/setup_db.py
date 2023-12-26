from ..schedular.utility.execute_query import QueryExecutor
import traceback

try:
    sql_query = open('DDL/db.sql').read()

    query_executor = QueryExecutor()
    conn = query_executor.create_cursor_connection()
    curr = conn.cursor()

    curr.execute(sql_query)
    conn.commit()

except Exception as e:
    # Handle the exception
    print(f"An error occurred: {str(e)}")
    traceback.print_exc()

finally:
    # Make sure to close the cursor and connection in the finally block
    try:
        if curr:
            curr.close()
        if conn:
            conn.close()
    except Exception as e:
        print(f"Error closing cursor/connection: {str(e)}")
