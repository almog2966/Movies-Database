import queries_db_script as queries
import mysql.connector
from mysql.connector import Error
import create_db_script as create
import api_data_retrieve as buildatabase
import sys
from kaggle.api.kaggle_api_extended import KaggleApi
import os
import zipfile

script_dir = os.path.dirname(os.path.abspath(__file__))  # Directory of script.py
project_dir = os.path.dirname(script_dir)  # Root directory of the project
makecsv_path = os.path.join(project_dir, 'DOCUMENTATION', 'creating_table')  # Path to the makecsv.py directory

# Add the directory containing makecsv.py to sys.path
sys.path.append(makecsv_path)

import makecsv

def download_files():
    api = KaggleApi()
    api.authenticate()

    # Navigate up two levels from DOC/create to main_folder, then to tables/input_table
    destination_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'tables', 'input_tables')
    destination_path = os.path.normpath(destination_path)
    dataset_path = 'rounakbanik/the-movies-dataset'
    files_to_download = ['ratings.csv', 'keywords.csv', 'movies_metadata.csv']

    for file_name in files_to_download:
        api.dataset_download_file(dataset_path, file_name, path=destination_path, force=True)

    for file_name in files_to_download:
        local_zip_file = os.path.join(destination_path, file_name + '.zip')
        if os.path.exists(local_zip_file):
            with zipfile.ZipFile(local_zip_file, 'r') as zip_ref:
                zip_ref.extractall(destination_path)
            os.remove(local_zip_file)
    print("download done!")
def create_full_cycle_database():
    # full downloading dataset, filtering and creating new csv files. creating SQL databse tables and filling the tables with data
    download_files() # Download dataset from kaggle API to tables/input_tables
    makecsv.main() # filter and the csv files and create new csv files in tables/output_tables
    create.create_database() # create empty database tables
    buildatabase.fillTables() # filling the database tablels

def main():
    #create_full_cycle_database()
    try:
        cnx = mysql.connector.connect(user='almogabudi',
                                    password='almogabu14007',
                                    host='127.0.0.1',
                                    database='almogabudi',
                                    port=3305)
        cursor = cnx.cursor()

        print("query 1 example:")
        # Example of how to call one of the functions
        rows = queries.query_1(cursor,"Berlin")
        columns = [column[0] for column in cursor.description]
        print("\t".join(columns))  # Print column names
        for row in rows:
            print("\t".join(str(value) for value in row))

        print("query 2 example:")
        rows = queries.query_2(cursor,"Walden Media")
        columns = [column[0] for column in cursor.description]
        print("\t".join(columns))  # Print column names
        for row in rows:
            print("\t".join(str(value) for value in row))

        print("query 3 example:")
        rows = queries.query_3(cursor,"vampire",50,200)
        columns = [column[0] for column in cursor.description]
        print("\t".join(columns))  # Print column names
        for row in rows:
            print("\t".join(str(value) for value in row))

        print("query 4 example:")
        rows = queries.query_4(cursor,"en")
        columns = [column[0] for column in cursor.description]
        print("\t".join(columns))  # Print column names
        for row in rows:
            print("\t".join(str(value) for value in row))

        print("query 5 example:")
        rows = queries.query_5(cursor,"en")
        columns = [column[0] for column in cursor.description]
        print("\t".join(columns))  # Print column names
        for row in rows:
            print("\t".join(str(value) for value in row))

        print("query 6 example:")
        rows = queries.query_6(cursor)
        columns = [column[0] for column in cursor.description]
        print("\t".join(columns))  # Print column names
        for row in rows:
            print("\t".join(str(value) for value in row))


    except Error as e:
        print(f"An Error has occurred: {e}")

    finally:
    # Ensure the cursor and connection are closed properly
        if 'cursor' in locals():
            cursor.close()
        if 'cnx' in locals() and cnx.is_connected():
            cnx.close()

if __name__ == "__main__":
    main()