import pandas as pd
import io
import psycopg2

from typing import List

def fast_pg_insert(df: pd.DataFrame, connection: str, table_name: str, columns: List[str]) -> None:
    """
        Inserts data from a pandas DataFrame into a PostgreSQL table using the COPY command for fast insertion.

        Parameters:
        df (pd.DataFrame): The DataFrame containing the data to be inserted.
        connection (str): The connection string to the PostgreSQL database.
        table_name (str): The name of the target table in the PostgreSQL database.
        columns (List[str]): A list of column names in the target table that correspond to the DataFrame columns.

        Returns:
        None
    """
    conn = psycopg2.connect(connection)
    _buffer = io.StringIO()
    df.to_csv(_buffer, sep=";", index=False, header=False)
    _buffer.seek(0)
    with conn.cursor() as c:
        c.copy_from(
            file=_buffer,
            table=table_name,
            sep=";",
            columns=columns,
            null=''
        )
    conn.commit()
    conn.close()