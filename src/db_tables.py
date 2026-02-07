from sqlalchemy import inspect
from .db_connections import get_connection

def get_db_tables():
    
    conn = get_connection()
    insp = inspect(conn)
    tables = insp.get_table_names()

    return tables