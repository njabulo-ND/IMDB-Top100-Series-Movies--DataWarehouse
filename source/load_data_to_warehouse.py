import pandas as pd
from sqlalchemy import create_engine,text
from sqlalchemy.exc import SQLAlchemyError,OperationalError
import urllib.parse
import json
def python_to_sql(sever_name,database):
    """
    Connects to a SQL Server database, loads JSON data into a table, 
    alters column types, and fetches the top 10 rows to validate the creation of the table.

    Parameters:
    - server_name (str): SQL Server instance (must end with 'SQLEXPRESS')
    - database (str): Target database name

    Raises:
    - ValueError: If the server name is not a SQL Server Express instance
    """
    try:
         # Validate server: only accept SQL Server Express (SSMS)
        if not sever_name.endswith('SQLEXPRESS'):
            raise ValueError('Wrong sever name only accept SQL SEVER wich is SSMS')
        driver_name = r'ODBC Driver 17 for SQL Server'

        # Connection parameters
        connection_string = (
            f"DRIVER={{{driver_name}}};"
            f"SERVER={sever_name};"
            f"DATABASE={database};"
            f"Trusted_Connection=yes;"
        )
        # Encode the connection string for SQLAlchemy
        fixed_connection = urllib.parse.quote_plus(connection_string)
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={fixed_connection}")

        # Load JSON data into pandas
        read_json = pd.read_json('data/cleaned_top100_list.json')

        # Write data to SQL table (replace if exists)
        read_json.to_sql('Top100_movies', engine, if_exists='replace', index=False)

        # Alter the 'rating' column to FLOAT type
        with engine.begin() as connecting_to_DB:
            connecting_to_DB.execute(text(
                """
                    ALTER TABLE Top100_movies
                    ALTER COLUMN rating FLOAT;
                """
            ))
        
            SQL_query = """
                    SELECT * FROM Top100_movies
        """
            
        # Fetch data into pandas DataFrame
        table_from_sql = pd.read_sql(SQL_query,engine)
        # Display basic information and top 10 rows (columns 0 and 1) just to confirm successfulness of the table creation
        print(table_from_sql.shape)
        print(table_from_sql.dtypes)
        print(table_from_sql.iloc[:10,[0,1]])
        
    #I group related exceptions under a single handler to simplify error management and improve code clarity.
    except SQLAlchemyError as e:
        print(f'Error occured:\n{e}')
    except json.JSONDecodeError as e:
        print(f'Error occured:\n{e}')
    except FileNotFoundError as e:
        print(f'file not found:\n{e}')
    except Exception as e:
        print(f'Error occured:\n{e}')
    except OperationalError as e:
        print(f'Error occured:\n{e}')
        
# Example usage
sever_name = r'njabulo\SQLEXPRESS'
database = 'IMDB_WAREHOUSE'
python_to_sql(sever_name,database)
