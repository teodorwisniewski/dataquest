#imports

import psycopg2
import csv
import os
import pandas as pd
import numpy as np

# internal imports
from load_data_to_pandas_from_postgres_database import read_sql_table_to_df



def check_connection(conn):
    flag = conn.closed
    flag_str = "closed" if flag else "open"
    return f"The connection to the db is {flag_str}"





hostname = 'localhost' #os.environ.get('HOST_POSTGRES')
username = 'postgres' #os.environ.get('DB_USER')
password = os.environ.get('DB_PASS_LOCAL') #os.environ.get('DB_PASS') #
database = 'postgres'
schema_name = 'public'
table_name = 'users'


# In[31]:

try:
    conn = psycopg2.connect( host=hostname, user=username, password=password, dbname=database )


    # In[28]:


    # row_values = {
    #     'identifier': 25,
    #     'email': 'Jose.Kirby@dataquest.io',
    #     'name': 'Jose Kirby',
    #     'address': '5 random street'
    # }


    # cur = conn.cursor()
    # placeholders = ', '.join(['%('+val+')s' for val in row_values])
    # cur.execute(f"""
    #     INSERT INTO users VALUES (
    #     {placeholders}
    #     )""",row_values)
    # conn.commit()


    # 2/9 unwanted sql injections
    def get_email(name):
        cur = conn.cursor()
        # create the query string using the format function
        query_string = "SELECT email FROM users WHERE name = '" + name + "';"
        # execute the query
        cur.execute(query_string)
        res = cur.fetchall()
        return res
    # add you code below

    # example of sql injections that gives us more information than we whould obtain
    fake_name = "JAnusz' OR 0=0; --"
    all_emails = get_email(fake_name)
    print(all_emails)

    """
    3/9  Tricking the sql database: how to extract in a clever way more data
    than we should. We can use union command to do it as union command allows 
    to get extra rows as long as the column number is correct
    """
    dirty_sql_injection = """"Joseph Kirby' 
    UNION
    SELECT address FROM users WHERE name= 'Joseph Kirby'
    UNION
    SELECT CAST(id AS varchar) FROM users WHERE name= 'Joseph Kirby"""
    print("\n\n\n\n\ Part 3/9 ")
    getting_rows = get_email(dirty_sql_injection)
    print(getting_rows)
    name = "Larry Cain' UNION SELECT address FROM users WHERE name = 'Larry Cain"
    output = get_email(name)
    print(output)


    """
    4/9  
    We managed to avoid inserting unwanted sql injections from clever users
    with the construction cur.excecute('sql query %s %s', argument_values)
    Result from the code below
   [('larry.cain@martinez.net',)]
    []
    """
    print("\n\n\n\n\ Part 4/9 ")
    def get_email_fixed(name):
       cur = conn.cursor()
       cur.execute("SELECT email FROM users WHERE name = %s", (name,))
       res = cur.fetchall()
       return res


    name = "Larry Cain"
    output = get_email_fixed(name)
    print(output)
    name = "Larry Cain' UNION SELECT address FROM users WHERE name = 'Larry Cain"
    output = get_email_fixed(name)
    print(output)
    print("\n\n\n\n")

    # printing results of the database
    print(check_connection(conn))
    #
    df = read_sql_table_to_df(conn, schema_name, table_name)
    print(df.head())



except Exception as err:
    print("Oops! An exception has occured:",  str(err))
    print("Exception TYPE:", type(err))
finally:
    conn.close()
print(check_connection(conn))








