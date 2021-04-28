import os
import pandas as pd
import psycopg2


def do_query(conn:psycopg2.extensions.connection,
             schema_name:str,
             table_name:str) -> pd.DataFrame:
    cur = conn.cursor()
    cur.execute( f"SELECT * FROM {schema_name}.{table_name}" )
    query_output_df = get_df_from_sql_table(cur)
    return query_output_df


def get_df_from_sql_table(cursor: psycopg2.extensions.cursor) -> pd.DataFrame:
    """
    this function function takes the cursor object and
    adapt its output to the dataframe object
    :param data: cursor
    :param headers: cursor
    :return:
    """
    data = cursor.fetchall()
    headers= cursor.description
    df_from_sql = pd.DataFrame(data=data, columns=[i[0] for i in headers])
    return df_from_sql


def read_sql_table_to_df(connection:psycopg2.extensions.connection,
                         schema_name:str,
                         table_name:str) -> pd.DataFrame:
    """
    :param connection: psycopg2 connection
    :param schema_name: schema name in the postgres database
    :param table_name: table name  in the postgres database
    :return:
    """

    with connection as conn:
        df = do_query( connection, schema_name , table_name)
    return df


if __name__ == "__main__":
    # List of variables that have to be defined by users of this script.
    hostname = os.environ.get('HOST_POSTGRES')
    username = os.environ.get('DB_USER')
    password = os.environ.get('DB_PASS')  #
    database = os.environ.get('DATABASE_NAME_POSTGRES')
    schema_name = 'public'
    table_name = 'happiness2021'
    my_connection = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
    df = read_sql_table_to_df(my_connection, schema_name, table_name)
    print(df.head())
