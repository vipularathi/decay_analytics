import pandas as pd
import sqlalchemy as sql
from sqlalchemy import create_engine, inspect
from urllib.parse import quote_plus
import psycopg2
import re

remote_host = "172.16.47.56"
remote_dbname = "TBTMData"
remote_user = "postgres"
remote_password = "Rathi123"

try:
    remote_engine = create_engine(f'postgresql+psycopg2://{remote_user}:{remote_password}@{remote_host}:5432/{remote_dbname}')

    query = '''
        select table_name from information_schema.tables
        where table_schema = 'public'
    '''

    df = pd.read_sql(query, remote_engine)
    df.to_csv(f'table_list.csv', index=False)
    print(df)

    df1 = df.copy()
    # df1['table_name'] = df1['table_name'].apply(lambda x: x if x.endswith('_fo_1M') else None)
    df1['table_name'] = df1['table_name'].apply(lambda x: x if isinstance(x, str) and x.endswith('_fo_1M') else None)
    # df1 = df1[(df1['table_name'] != None)]
    # df1 = df1[df1['table_name'].endswith('_fo_1M')]
    # df1.to_csv(f'fo_table_list.csv', index=False )
    # df1 = df1.query('table_name == None')
    df1 = df1[df1['table_name'].notna()]
    df_list = df1['table_name'].tolist()
    # print(f'{df1}\n{type(df1.tolist())}')
    pattern =
    print(df_list)

except psycopg2.Error as e:
    print(f'Error is {e}')