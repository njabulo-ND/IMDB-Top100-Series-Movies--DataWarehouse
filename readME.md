# 🎬 IMDB Top 100 Movies & Series ETL Pipeline

## Overview
This project demonstrates a **full end-to-end ETL (Extract, Transform, Load) pipeline** for handling and analyzing structured JSON data from the **Top 100 Movies and Series** datasets.  

It showcases best practices in **data validation, transformation, normalization, and database integration**, highlighting how raw JSON datasets can be cleaned, flattened, normalized, and loaded into a **SQL Server data warehouse** for further analysis.  

The main goal is to illustrate **relational data modeling**, solving **1:1 and many-to-many relationships** between movies/series and their genres.

---

## Project Structure

### Root Level
```
📂 Root level
│
├─ .env                          # Environment variables for secure connection strings
├─ requirements.txt               # Python dependencies
```

### Virtual Environment
```
📂 venv                          # Virtual environment (not expanded)
```

### Data Folder
```
📂 data
│
├─ clean_moviedata_byID.json
├─ clean_seriesdata_byID.json
├─ raw_moviedata_byID.json
├─ raw_seriesdata_byID.json
├─ cleaned_top100_movies_list.json
└─ cleaned_top100_series_list.json
```

### Source Code Folder
```
📂 source
│
├─ Extract_FIND_API_to_raw_JSON.py   # Extracts data from API and saves raw JSON
├─ Process_data.py                   # Cleans, normalizes, and transforms data
└─ load_data_to_warehouse.py         # Loads data into SQL Server and manages relationships
```

---

## ETL Workflow

### 1️⃣ Extraction
- Load raw JSON files from the `data/` folder:
  - Top 100 Movies (`cleaned_top100_movies_list.json`)  
  - Movies by ID (`clean_moviedata_byID.json`)  
  - Top 100 Series (`cleaned_top100_series_list.json`)  
  - Series by ID (`clean_seriesdata_byID.json`)  
- Perform **file validation and error handling** for missing or malformed JSON.

### 2️⃣ Transformation
- **Validation & Cleaning**
  - Verify schema consistency across rows.
  - Handle missing or empty values.
  - Flatten nested lists, tuples, sets, and dictionaries into strings for database storage.
- **Normalization**
  - Solve **many-to-many relationships** between movies/series and genres:
    - Create separate `Genres` tables.
    - Create mapping tables (`Movies_Genres_map`, `Series_Genre_Map`).
- **1:1 Relationships**
  - Link detailed movie/series data to the main Top 100 tables using primary keys.

### 3️⃣ Loading
- Connect to **SQL Server Express** using SQLAlchemy and ODBC.
- Create and normalize the following tables:
  - `Top100_movies`, `Movies_by_Id`, `Movies_Genres`, `Movies_Genres_map`
  - `Top100_series`, `Series_by_Id`, `Series_Genres`, `Series_Genre_Map`
- Implement **primary and foreign keys** to maintain relational integrity.
- Insert cleaned and normalized data into the database.
- Validate table creation and data insertion via SQL queries.

---

## Technical Highlights
- **Language & Libraries**: Python, Pandas, SQLAlchemy, pyODBC, JSON.
- **Database**: SQL Server Express (local instance).
- **Data Modeling**:
  - **Many-to-Many**: Movie/Series ↔ Genres via associative tables.
  - **One-to-One**: Detailed movie/series info ↔ Top 100 tables.
- **Error Handling**: Robust handling for file, SQL, JSON, and operational errors.
- **Scalability**: Designed to handle large JSON datasets with reusable functions.
- **Best Practices**:
  - Flattening and cleaning nested structures.
  - Dropping duplicates and handling missing values.
  - Use of environment-safe connection strings recommended for production.

---

## 🚀 Project Purpose & Showcase

This project demonstrates **industry-ready data engineering skills** through an end-to-end ETL pipeline for top 100 movies and series datasets. It highlights the ability to handle **real-world data challenges** and deliver production-ready datasets for analytics or AI applications.

### Key Highlights

1. **Full-Stack Data Engineering**
   - Built a complete ETL pipeline: extraction from APIs, transformation of nested JSON, and loading into a SQL Server data warehouse.
   - Automated data validation, cleaning, and deduplication to ensure **high-quality, reliable datasets**.
   - Normalized complex M:N relationships with **associative tables** for scalable analytics.

2. **Advanced Database & SQL Expertise**
   - Designed **relational schemas** with primary, foreign, and composite keys.
   - Managed **table dependencies and integrity constraints** for robust data pipelines.
   - Created analytical-ready tables supporting reporting, queries, or machine learning pipelines.

3. **Python & Pandas Mastery**
   - Transformed complex, nested JSON using **pandas**: exploding lists, merging datasets, and cleaning data efficiently.
   - Developed **modular, reusable scripts** with clear function separation.
   - Implemented **robust exception handling** for real-world data reliability.

4. **Professional, Production-Ready Workflow**
   - Organized project structure with virtual environments, `.env` configuration, and clean folder hierarchy.
   - Demonstrates ability to **prepare datasets and pipelines for real-world business scenarios**.
   - Shows readiness to contribute immediately in **data engineering or analytics teams**.

---

## How to Use

1. Clone the repository.
2. Set up a **SQL Server Express instance**.
3. Configure `.env` for database credentials (optional but recommended).
4. Place raw JSON files in the `data/` folder.
5. Run scripts sequentially from the `source/` folder:
   ```bash
   python Extract_FIND_API_to_raw_JSON.py
   python Process_data.py
   python load_data_to_warehouse.py

Verify database tables using SQL queries or Pandas read_sql.

Note: The Movies_by_Id and Series_by_Id tables store data for the top 100 movies/series. In this project, only a subset of rows was retrieved for demonstration purposes, but the tables can hold and retrieve data for any of the top 100 entries.

---

## Future Enhancements
- Add automated **unit testing** for data validation.
- Schedule ETL using **Airflow** or **Prefect**.
- Add **logging and monitoring** for production-level ETL.
- Extend the pipeline to include **ratings, reviews, and box office data**.

---

## Conclusion
This project is a **comprehensive demonstration of ETL and relational data modeling** using Python and SQL Server.  

It illustrates how raw JSON data can be transformed into a **normalized, query-ready relational database**, showcasing skills in **data engineering, Python programming, and database management**.