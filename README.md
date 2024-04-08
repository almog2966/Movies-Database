# SQL Movie Database Project

## Overview

This project aims to create an efficient SQL database for movie data, leveraging Python and Pandas for data manipulation. The database structure is designed to support complex queries related to movie metadata, genres, ratings, keywords, and production companies. This project is built using data from the [Movies Dataset on Kaggle](https://www.kaggle.com/datasets/rounakbanik/the-movies-dataset).

## Database Structure

The database consists of the following tables and relationships:

- **Movies_metadata**: Contains comprehensive information about movies.
- **Genres**: Lists all movie genres.
- **Movie_to_genres**: Maps movies to their genres.
- **Ratings**: Holds movie ratings.
- **Keywords**: Associates movies with keywords.
- **Production_companies**: Lists production companies.
- **Movie_to_companies**: Maps movies to their production companies.

Foreign keys ensure data integrity across relationships.

## Implemented Queries

The project includes several queries for extracting specific information:

1. **Keyword Search**: Finds top 5 rated movies containing a given keyword.
2. **Production Company Insights**: Lists production companies with movie counts and average popularity.
3. **Family-Friendly Movies by Overview**: Retrieves movies suitable for families based on overview and time range.
4. **Top Rated Movies by Language and Genre**: Finds top 5 rated movies for each genre in a given language.
5. **Top Revenue Movies by Language and Genre**: Similar to the previous, but sorts by revenue.
6. **Widely Loved Movies**: Lists all movies with a rating above 4 in multiple genres.

## Project Structure

- `makecsv.py`: Prepares the raw CSV file for database integration.
- `create_db_script.py`: Creates the database schema if it does not exist.
- `api_data_retrieve.py`: Populates the database with data from CSV files.
- `queries_db_script.py`: Contains the SQL queries as functions.
- `queries_execution.py`: Automates data retrieval, database creation, and query execution.

## Getting Started

1. **Prerequisites**:Ensure creating Python virtual env and install all requirements in the requirements.txt file.
2. **Data Preparation**: Download the dataset from Kaggle and use `makecsv.py` to prepare the data.
3. **Database Creation**: Run `create_db_script.py` to set up the database schema.
4. **Data Import**: Execute `api_data_retrieve.py` to populate the database.
5. **Query Execution**: Use `queries_execution.py` to run the predefined queries.

**OR**
1. **Prerequisites**:Ensure creating Python virtual env and install all requirements in the requirements.txt file.
2. **Run Noted FUnction**: In queries_execution.py unnote the create_full_cycle_database() which will download the dataset from kaggle API, will create the DB, fill it and run the queries.


## License

This project is open-sourced under the [MIT License](LICENSE.md).

## Acknowledgments

- Data provided by the [Movies Dataset on Kaggle](https://www.kaggle.com/datasets/rounakbanik/the-movies-dataset).
- Special thanks to [any contributors, mentors, or entities].
