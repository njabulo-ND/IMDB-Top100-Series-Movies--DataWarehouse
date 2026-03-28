import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError, OperationalError
import urllib.parse
import json
from dotenv import load_dotenv
import os

# Fetching key from env 
try:
    project_dir = os.getcwd()
    env_path = os.path.join(project_dir, ".env")
    load_dotenv(dotenv_path=env_path)
    if os.getenv("SQL_SERVER") and os.getenv("SQL_DB"):
        print('Server and Database found')
        server_name = os.getenv("SQL_SERVER")
        database = os.getenv("SQL_DB")
    else:
        raise OSError('Database and Sever not found')
except OSError as e:
    print(f'OS error related occured: \n{e}')
except Exception as e:
    print(f"Error occured:\n{e}")

"""Loading the top 100 movie data to the database,normalizing the table and also solving the M:N relationship by assossiative table"""
def top100list_movies_to_sql(sever_name, database, file_to_load_from):
    """
    Connects to a SQL Server database, loads JSON data into a table, 
    alters column types, and fetches the top 10 rows to validate the creation of the table.

    Parameters:
    - server_name (str): SQL Server instance (must end with 'SQLEXPRESS')
    - database (str): Target database name

    Raises:
    - ValueError: If the server name is not a SQL Server Express instance

    Return:
    -The table from SQL to validate if the table was successfully created and it exists on SQL
    """
    try:
        # Validate server: only accept SQL Server Express (SSMS)
        if not sever_name.endswith('SQLEXPRESS'):
            raise ValueError(
                'Wrong sever name only accept SQL SEVER wich is SSMS')
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
        engine = create_engine(
            f"mssql+pyodbc:///?odbc_connect={fixed_connection}")

        # Load JSON data into pandas
        try:
            read_json = pd.read_json(file_to_load_from)
            read_json = read_json.drop_duplicates(subset=['id'], keep='first')
            read_json = read_json.dropna(subset=['id'])
        except FileNotFoundError:
            print(f'File:{file_to_load_from} not found')
        except Exception as e:
            raise ValueError(f'Error occured: {e}')
        #Droping all child tables depending on this table to successfully replace the table if it exists
        with engine.begin() as conn:
                conn.execute(text("""
                    DROP TABLE IF EXISTS Movies_Genres_map;
                    DROP TABLE IF EXISTS Movies_Genres;             
                    DROP TABLE IF EXISTS Movies_by_Id;
                    DROP TABLE IF EXISTS Top100_movies;
                """))
        # Write data to SQL table (replace if exists)
        read_json.to_sql('Top100_movies', engine,
                         if_exists='replace', index=False)

        # Alter the 'rating' column to FLOAT type

        with engine.begin() as connecting_to_DB:
            connecting_to_DB.execute(text("""
                    ALTER TABLE Top100_movies
                    ALTER COLUMN id VARCHAR(20) NOT NULL
                """))
            connecting_to_DB.execute(text(
                """
                        ALTER TABLE Top100_movies
                        ADD CONSTRAINT id_to_pk PRIMARY KEY(id) ;
                   """
            ))

        # NORMALIZE GENRES
        # Split genres
        read_json['genre'] = read_json['genre'].str.split(',')

        # Explode rows
        read_json_exploded = read_json.explode('genre')

        # Clean spaces
        read_json_exploded['genre'] = read_json_exploded['genre'].str.strip()

        #Creating genres table to normalize the top100 movies table
        genres_df = pd.DataFrame(read_json_exploded['genre'].unique(), columns=['genre_name'])

        genres_df.to_sql('Movies_Genres', engine, if_exists='replace', index=False)

        with engine.begin() as conn:
            conn.execute(text("""
                ALTER TABLE Movies_Genres
                ADD genre_id INT IDENTITY(1,1) PRIMARY KEY;
            """))

        #Creating the composite table to break M:N relationship between Movies and genres
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE Movies_Genres_map(
                    movie_id VARCHAR(20),
                    genre_id INT,
                    PRIMARY KEY (movie_id, genre_id),
                    FOREIGN KEY (movie_id) REFERENCES Top100_movies(id),
                    FOREIGN KEY (genre_id) REFERENCES Movies_Genres(genre_id)
                );
            """))

       #Mapping GENRES to IDs
        genres_from_db = pd.read_sql("SELECT * FROM Movies_Genres", engine)

        movie_genres = read_json_exploded.merge(
            genres_from_db,
            left_on='genre',
            right_on='genre_name'
        )

        movie_genres = movie_genres[['id', 'genre_id']]
        movie_genres.columns = ['movie_id', 'genre_id']

        # Remove duplicates just in case
        movie_genres = movie_genres.drop_duplicates()

        # Insert data into my composite table table
        movie_genres.to_sql('Movies_Genres_map', engine, if_exists='append', index=False)

        #DROP GENRE COLUMN from the parent table top100movies
        with engine.begin() as conn:
            conn.execute(text("""
                ALTER TABLE Top100_movies
                DROP COLUMN genre;
            """))

        # Validating my normalization
        movies = pd.read_sql("SELECT * FROM Top100_movies", engine)
        genres = pd.read_sql("SELECT * FROM Movies_Genres", engine)
        movie_genres = pd.read_sql("SELECT * FROM Movies_Genres_map", engine)

        return movies, genres, movie_genres
    
    # I group related exceptions under a single handler to simplify error management and improve code clarity.
    except SQLAlchemyError as e:
        print(f'Error occured:\n{e}')
        return None, None, None
    except json.JSONDecodeError as e:
        print(f'Error occured:\n{e}')
        return None, None, None
    except FileNotFoundError as e:
        print(f'file not found:\n{e}')
        return None, None, None
    except OperationalError as e:
        print(f'Error occured:\n{e}')
        return None, None, None
    except Exception as e:
        print(f'Error occured:\n{e}')
        return None, None, None
# Example usage
# try:
#     file =r'data/cleaned_top100_movies_list.json'
#     movies, genres, movie_genres = top100list_movies_to_sql(server_name, database, file)
#     print(movies.shape)
#     print(genres.shape)
#     print(movie_genres.shape)
# except Exception as e:
#     print(f'Error occured:\n{e}')

"""Loading the movies data by movies id to the database and creating a 1:1 relation ship between it and the top 100 movies table"""
def movie_byID_tosql(sever_name, database, file_to_load_from):
    """
    Connects to a SQL Server database, loads JSON data into a table, 
    link table to the main 100 movies table, and fetches few rows to validate the creation of the table.

    Parameters:
    - server_name (str): SQL Server instance (must end with 'SQLEXPRESS')
    - database (str): Target database name

    Raises:
    - ValueError: If the server name is not a SQL Server Express instance
    """
    try:
        # Validate server: only accept SQL Server Express (SSMS)
        if not sever_name.endswith('SQLEXPRESS'):
            raise ValueError(
                'Wrong sever name only accept SQL SEVER wich is SSMS')
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
        engine = create_engine(
            f"mssql+pyodbc:///?odbc_connect={fixed_connection}")

        # Load JSON data into pandas
        try:
            read_json = pd.read_json(file_to_load_from)
            read_json = read_json.drop_duplicates(subset=['id'], keep='first')
            read_json = read_json.dropna(subset=['id'])
        except FileNotFoundError:
            print(f'File:{file_to_load_from} not found')
        except Exception as e:
            raise ValueError(f'Error occured: {e}')
        # Write data to SQL table (replace if exists)
        read_json.to_sql('Movies_by_Id', engine,
                         if_exists='replace', index=False)

        # Alter the the table to edit and define the relationship with the main movie table
        with engine.begin() as connecting_to_DB:
            connecting_to_DB.execute(text(
                """
                    ALTER TABLE Movies_by_Id
                    ALTER COLUMN id VARCHAR(20) NOT NULL"""
            ))
            connecting_to_DB.execute(text(
                """
                    ALTER TABLE Movies_by_Id
                    ADD CONSTRAINT id_from_movies_by_id_to_pk 
                    PRIMARY KEY(id);
                """
            ))
            connecting_to_DB.execute(text(
                """
                    ALTER TABLE Movies_by_Id
                    ADD CONSTRAINT id_from_movies_by_id_to_fk 
                    FOREIGN KEY(id) REFERENCES Top100_movies(id);
                """
            ))

            SQL_query = """
                    SELECT * FROM Movies_by_Id
                    """

        # Fetch data into pandas DataFrame
        table_from_sql = pd.read_sql(SQL_query, engine)
        # Display basic information  to confirm successfulness of the table creation
        return table_from_sql

    # I group related exceptions under a single handler to simplify error management and improve code clarity.
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
# try:
#     file =r'data/clean_moviedata_byID.json'
#     df = movie_byID_tosql(server_name, database, file)
#     print(df.head())
#     print(df.shape)
# except Exception as e:
#     print(f'Error occured:\n{e}')

"""Loading the top 100 series data to the database,normalizing the table and also solving the M:N relationship by assossiative table"""
def top100list_series_to_sql(sever_name, database, file_to_load_from):
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
            raise ValueError(
                'Wrong sever name only accept SQL SEVER wich is SSMS')
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
        engine = create_engine(
            f"mssql+pyodbc:///?odbc_connect={fixed_connection}")

        # Load JSON data into pandas
        try:
            read_json = pd.read_json(file_to_load_from)
            read_json = read_json.drop_duplicates(subset=['id'], keep='first')
            read_json = read_json.dropna(subset=['id'])
        except FileNotFoundError:
            print(f'File:{file_to_load_from} not found')
        except Exception as e:
            raise ValueError(f'Error occured: {e}')
        #Droping all child tables depending on this table to successfully replace the table if it exists
        with engine.begin() as conn:
                conn.execute(text("""
                    DROP TABLE IF EXISTS Series_Genre_Map;
                    DROP TABLE IF EXISTS Series_by_Id;
                    DROP TABLE IF EXISTS Series_Genres;
                    DROP TABLE IF EXISTS Top100_series;
                """))
        # Write data to SQL table (replace if exists)
        read_json.to_sql('Top100_series', engine,
                         if_exists='replace', index=False)

        # Alter the id column and make PK for further references
        with engine.begin() as connecting_to_DB:
            connecting_to_DB.execute(text("""
                    ALTER TABLE Top100_series
                    ALTER COLUMN id VARCHAR(20) NOT NULL
                """))
            connecting_to_DB.execute(text(
                """
                        ALTER TABLE Top100_series
                        ADD CONSTRAINT series_id_to_pk PRIMARY KEY(id) ;
                   """
            ))
        # NORMALIZE GENRES using the same logic for Series table as i did on movies
        # Split genres
        read_json['genre'] = read_json['genre'].str.split(',')
        # Explode rows
        read_json_exploded = read_json.explode('genre')
        # Clean spaces
        read_json_exploded['genre'] = read_json_exploded['genre'].str.strip()

        #Creating genres table to normalize the top100 series table
        genres_df = pd.DataFrame(read_json_exploded['genre'].unique(), columns=['genre_name'])
        genres_df.to_sql('Series_Genres', engine, if_exists='replace', index=False)
        with engine.begin() as connection_sql:
            connection_sql.execute(text("""
                ALTER TABLE Series_Genres
                ADD genre_id INT IDENTITY(1,1) PRIMARY KEY;
            """))

        #Creating the composite table to break M:N relationship between Series and genres
        with engine.begin() as connection_sql:
            connection_sql.execute(text("""
                CREATE TABLE Series_Genre_Map (
                    Series_id VARCHAR(20),
                    genre_id INT,
                    PRIMARY KEY (Series_id, genre_id),
                    FOREIGN KEY (Series_id) REFERENCES Top100_series(id),
                    FOREIGN KEY (genre_id) REFERENCES Series_Genres(genre_id)
                );
            """))

       #Mapping GENRES to IDs
        genres_from_db = pd.read_sql("SELECT * FROM Series_Genres", engine)

        series_genres = read_json_exploded.merge(
            genres_from_db,
            left_on='genre',
            right_on='genre_name'
        )

        series_genres = series_genres[['id', 'genre_id']]
        series_genres.columns = ['Series_id', 'genre_id']

        # Remove duplicates just in case
        series_genres = series_genres.drop_duplicates()

        # Insert data into my composite table table
        series_genres.to_sql('Series_Genre_Map', engine, if_exists='append', index=False)

        #DROP GENRE COLUMN from the parent table top100movies
        with engine.begin() as connection_sql:
            connection_sql.execute(text("""
                ALTER TABLE Top100_series
                DROP COLUMN genre;
            """))

        # Validating my normalization
        series = pd.read_sql("SELECT * FROM Top100_series", engine)
        genres = pd.read_sql("SELECT * FROM Series_Genres", engine)
        series_genres = pd.read_sql("SELECT * FROM Series_Genre_Map", engine)
        return series, genres, series_genres
    
    # I group related exceptions under a single handler to simplify error management and improve code clarity.
    except SQLAlchemyError as e:
        print(f'Error occured:\n{e}')
        return None, None, None
    except json.JSONDecodeError as e:
        print(f'Error occured:\n{e}')
        return None, None, None
    except FileNotFoundError as e:
        print(f'file not found:\n{e}')
        return None, None, None
    except Exception as e:
        print(f'Error occured:\n{e}')
        return None, None, None
    except OperationalError as e:
        print(f'Error occured:\n{e}')
        return None, None, None
# Example usage
# try:
#     file =r'data/cleaned_top100_series_list.json'
#     series, genres, series_genres = top100list_series_to_sql(server_name, database, file)
#     print(series.shape)
#     print(genres.shape)
#     print(series_genres.shape)
# except Exception as e:
#     print(f'Error occured:\n{e}')

"""Loading the series data by series id to the database and creating a 1:1 relation ship between it and the top 100 series table"""
def series_byID_tosql(sever_name, database, file_to_load_from):
    """
    Connects to a SQL Server database, loads JSON data into a table, 
    link table to the main 100 series table, and fetches few rows to validate the creation of the table.

    Parameters:
    - server_name (str): SQL Server instance (must end with 'SQLEXPRESS')
    - database (str): Target database name

    Raises:
    - ValueError: If the server name is not a SQL Server Express instance
    """
    try:
        # Validate server: only accept SQL Server Express (SSMS)
        if not sever_name.endswith('SQLEXPRESS'):
            raise ValueError(
                'Wrong sever name only accept SQL SEVER wich is SSMS')
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
        engine = create_engine(
            f"mssql+pyodbc:///?odbc_connect={fixed_connection}")

        # Load JSON data into pandas
        try:
            read_json = pd.read_json(file_to_load_from)
            read_json = read_json.drop_duplicates(subset=['id'], keep='first')
            read_json = read_json.dropna(subset=['id'])
        except FileNotFoundError:
            print(f'File:{file_to_load_from} not found')
        except Exception as e:
            raise ValueError(f'Error occured: {e}')

        # Write data to SQL table (replace if exists)
        read_json.to_sql('Series_by_Id', engine,
                         if_exists='replace', index=False)

        # Alter the the table to edit and define the relationship with the main movie table
        with engine.begin() as connecting_to_DB:
            connecting_to_DB.execute(text(
                """
                    ALTER TABLE Series_by_Id
                    ALTER COLUMN id VARCHAR(20) NOT NULL"""
            ))
            connecting_to_DB.execute(text(
                """
                    ALTER TABLE Series_by_Id
                    ADD CONSTRAINT id_from_series_by_id_to_pk 
                    PRIMARY KEY(id);
                """
            ))
            connecting_to_DB.execute(text(
                """
                    ALTER TABLE Series_by_Id
                    ADD CONSTRAINT id_from_series_by_id_to_fk 
                    FOREIGN KEY(id) REFERENCES Top100_series(id);
                """
            ))

            SQL_query = """
                    SELECT * FROM Series_by_Id
                    """

        # Fetch data into pandas DataFrame
        table_from_sql = pd.read_sql(SQL_query, engine)
        # Display basic information  to confirm successfulness of the table creation
        return table_from_sql

    # I group related exceptions under a single handler to simplify error management and improve code clarity.
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
# try:
#     file =r'data/clean_seriesdata_byID.json'
#     df = series_byID_tosql(server_name, database, file)
#     print(df.head())
#     print(df.shape)
# except Exception as e:
#     print(f'Error occured:\n{e}')