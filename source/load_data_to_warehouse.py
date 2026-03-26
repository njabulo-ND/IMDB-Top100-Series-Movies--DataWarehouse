import pandas as pd
from sqlalchemy import create_engine,text
import urllib.parse
import json

sever_name = r'njabulo\SQLEXPRESS'
database = 'IMDB_WAREHOUSE'
driver_name = r'ODBC Driver 17 for SQL Server'

connection_string = (
    f"DRIVER={{{driver_name}}};"
    f"SERVER={sever_name};"
    f"DATABASE={database};"
    f"Trusted_Connection=yes;"
)

fixed_connection = urllib.parse.quote_plus(connection_string)

engine = create_engine(f"mssql+pyodbc:///?odbc_connect={fixed_connection}")
read_json = pd.read_json('data/cleaned_top100_list.json')
read_json.to_sql('Top100_movies', engine, if_exists='replace', index=False)


SQL_query = """
            SELECT * FROM Top100_movies
"""
with engine.begin() as connecting_to_DB:
    connecting_to_DB.execute(text(
        """
            ALTER TABLE Top100_movies
            ALTER COLUMN rating FLOAT;
        """
    ))
table_from_sql = pd.read_sql(SQL_query,engine)
print(table_from_sql.shape)
print(table_from_sql.dtypes)
print(table_from_sql.iloc[:10],[0,1,2,3,4,5])