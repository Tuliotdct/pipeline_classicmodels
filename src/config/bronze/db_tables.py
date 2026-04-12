from sqlalchemy import inspect
from src.connections.db_connections import get_connection
import pandas as pd


def get_db_tables():
    
    conn = get_connection()
    insp = inspect(conn)
    tables = insp.get_table_names()

    df = pd.DataFrame(tables, columns=['table_names'])
    db_tables_txt = df.to_csv('src/config/db_tables.txt', index=False, header=False)
    

    return db_tables_txt
