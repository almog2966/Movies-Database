import csv
import mysql.connector
from mysql.connector import Error


def fillTables():
    try:
        # Connect to your MySQL database
        cnx = mysql.connector.connect(user='almogabudi',
                                    password='almogabu14007',
                                    host='127.0.0.1',
                                    database='almogabudi',
                                    port=3305)
        cursor = cnx.cursor()

        # Path to your CSV file
        csv_file_path_list = ['tables/output_tables/movies metadata.csv',
                            'tables/output_tables/genres.csv',
                            'tables/output_tables/keywords.csv',
                            'tables/output_tables/movie-genre.csv',
                            'tables/output_tables/ratings.csv',
                            'tables/output_tables/production-companies.csv',
                            'tables/output_tables/movie-company.csv']
        queries_list = ["""
                INSERT INTO movies_metadata (adult, budget, movie_id, original_language, overview, popularity, release_date, revenue, runtime, tagline, title)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                """
                INSERT INTO genres (genre_id, name)
                VALUES (%s, %s)
                """,
                """
                INSERT INTO keywords (movie_id, keywords)
                VALUES (%s, %s)
                """,
                """
                INSERT INTO movie_to_genres (movie_id, genre_id)
                VALUES (%s, %s)
                """,
                """
                INSERT INTO ratings (movie_id, rating)
                VALUES (%s, %s)
                """,
                """
                INSERT INTO production_companies (name, company_id)
                VALUES (%s, %s)
                """,
                """
                INSERT INTO movie_to_companies (movie_id, company_id)
                VALUES (%s, %s)
                """
                ]

        # Open the CSV file
        for i in range(len(csv_file_path_list)):
            
            with open(csv_file_path_list[i], mode='r', encoding='utf-8') as csv_file:
                # Create a CSV reader
                csv_reader = csv.reader(csv_file)
                # Skip the header row
                next(csv_reader)
                
                for row in csv_reader:
                    # SQL query to insert data
                    insert_query = queries_list[i]
                    # Execute the insert query
                    try:
                        cursor.execute(insert_query, row)
                    except:
                        print("fail on:",row)
                # Commit the changes to the database
                cnx.commit()
        print("data retrieve done!")
        # Close the cursor and connection
    except mysql.connector.Error as err:
        print(f"Database error: {err}")
    except Exception as err:
        print(f"General error: {err}")
    finally:
        # Ensure the cursor and connection are closed properly
        if 'cursor' in locals():
            cursor.close()
        if 'cnx' in locals() and cnx.is_connected():
            cnx.close()



if __name__ == "__main__":
    fillTables()