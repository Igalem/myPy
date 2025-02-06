import pandas as pd
from io import StringIO
import psycopg2
import os


MAPPING_DF_TO_PG_FIELDS = {'int64': 'int', 'object' : 'text', 'float64': 'float'}


def _load_csv_to_dataframe(csv_filepath):
    try:
        df = pd.read_csv(csv_filepath, header=0)  ##, encoding='ISO-8859-8')
        df.insert(0, 'id', range(1, len(df) + 1))
        # df = df[['city', 'year', 'price']]


        # Trim spaces from all string columns
        df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

        return df
    except Exception as err:
        print('Failed to convert CSV file to Dataframe: ')
        print(err)


def _copy_csv_to_table(df, table_name, conn, batch_size=10000, schema='public'):
    """
    Copy data in batches for large datasets
    """
    cur = conn.cursor()
    try:
        # cur = conn.cursor()
        total_rows = len(df)

        for start_idx in range(0, total_rows, batch_size):
            end_idx = min(start_idx + batch_size, total_rows)
            batch_df = df.iloc[start_idx:end_idx]

            output = StringIO()
            batch_df.to_csv(output, sep=',', header=False, index=False)
            output.seek(0)

            cur.copy_expert(
                f"""
                COPY {schema}.{table_name} FROM STDIN WITH (
                    FORMAT CSV
                )
                """,
                output
            )

            conn.commit()
            print(f"Loaded rows {start_idx} to {end_idx}")
    except Exception as e:
        print(e)


def load_dataframe_to_postgres(csv_filepath, table_name, conn):
    df = _load_csv_to_dataframe(csv_filepath)
    print(df)

    schema = []
    for col, dtype in df.dtypes.items():
        column = col.replace(' ', '_').lower()
        field_type = MAPPING_DF_TO_PG_FIELDS[str(dtype)] if str(dtype) in MAPPING_DF_TO_PG_FIELDS else dtype
        schema.append(f"{column} {field_type}")

    query_create_table = f'CREATE TABLE {table_name} ({', '.join(schema)})'
    print(query_create_table)

    db_connection = psycopg2.connect(**conn)
    cur = db_connection.cursor()

    _copy_csv_to_table(df=df, table_name=table_name, conn=db_connection, schema='sbp')
    cur.close()





if __name__ == "__main__":
    POSTGRES_DB_USER = os.getenv('POSTGRES_DB_USER')
    POSTGRES_DB_PASSWORD = os.getenv('POSTGRES_DB_PASSWORD')
    POSTGRES_DB_NAME = os.getenv('POSTGRES_DB_NAME')

    # Database connection parameters
    connection_details = {
        'host': 'localhost',
        'port': '5432',
        'database': POSTGRES_DB_NAME,
        'user': POSTGRES_DB_USER,
        'password': POSTGRES_DB_PASSWORD
    }

    # Example usage
    csv_path = ''
    table_name = ''

    load_dataframe_to_postgres(csv_path, table_name=table_name, conn=connection_details)

