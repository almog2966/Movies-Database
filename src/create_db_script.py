import mysql.connector
from mysql.connector import errorcode

def create_database():
    TABLES = {}
    DB_NAME = 'Movies'
    TABLES['movies_metadata'] = ("""
        CREATE TABLE `movies_metadata` (
            `adult` VARCHAR(5),
            `budget` INT,
            `movie_id` SMALLINT PRIMARY KEY,
            `original_language` VARCHAR(3),
            `overview` TEXT,
            `popularity` DECIMAL(10,6),
            `release_date` DATE,
            `revenue` INT,
            `runtime` DECIMAL(6,3),
            `tagline` VARCHAR(255),
            `title` VARCHAR(100),
            FULLTEXT (`overview`),
            INDEX `idx_movies_runtime` (`runtime`),
            INDEX `idx_movies_popularity` (`popularity`)
        )ENGINE=InnoDB""")

    TABLES['genres'] = ("""    
        CREATE TABLE `genres` (
            `genre_id` SMALLINT PRIMARY KEY,
            `name` VARCHAR(20)
        )ENGINE=InnoDB""")

    TABLES['movie_to_genres'] = ("""
        CREATE TABLE `movie_to_genres` (
            `movie_id` SMALLINT,
            `genre_id` SMALLINT,
            PRIMARY KEY (`movie_id`, `genre_id`),
            FOREIGN KEY (`movie_id`) REFERENCES movies_metadata(`movie_id`),
            FOREIGN KEY (`genre_id`) REFERENCES genres(`genre_id`)
        )ENGINE=InnoDB""")

    TABLES['ratings'] = ("""
        CREATE TABLE `ratings` (
            `movie_id` SMALLINT,
            `rating` DECIMAL(5,4),
            PRIMARY KEY (`movie_id`, `rating`),
            FOREIGN KEY (`movie_id`) REFERENCES movies_metadata(`movie_id`),
            INDEX `idx_ratings_rating` (`rating`),
            INDEX `idx_ratings_movie_id` (`movie_id`)
        )ENGINE=InnoDB""")

    TABLES['keywords'] = ("""
        CREATE TABLE `keywords` (
            `movie_id` SMALLINT,
            `keywords` TEXT,
            FOREIGN KEY (`movie_id`) REFERENCES movies_metadata(`movie_id`),
            FULLTEXT (`keywords`)
        )ENGINE=InnoDB""")

    TABLES['production_companies'] = ("""
        CREATE TABLE `production_companies` (
            `name` TEXT,
            `company_id` INT PRIMARY KEY,
            FULLTEXT (`name`)
        )ENGINE=InnoDB""")

    TABLES['movie_to_companies'] = ("""
        CREATE TABLE `movie_to_companies` (
            `movie_id` SMALLINT,
            `company_id` INT,
            FOREIGN KEY (`movie_id`) REFERENCES movies_metadata(`movie_id`),
            FOREIGN KEY (`company_id`) REFERENCES production_companies(`company_id`),
            INDEX `idx_mtc_company_id` (`company_id`),
            INDEX `idx_mtc_movie_id` (`movie_id`)
        )ENGINE=InnoDB""")

    cnx = mysql.connector.connect(user='almogabudi',
                                  password='almogabu14007',
                                  host='localhost',
                                  database='almogabudi',
                                  port=3305)
    try:
        cursor = cnx.cursor()

        for table_name in TABLES:
            table_description = TABLES[table_name]
            try:
                cursor.execute(table_description)
            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                    print("already exists")
                else:
                    print(err.msg)
        print("create database Done!")
    except mysql.connector.Error as err:
        print(f"Database connection failed: {err}")
    finally:
        if cnx.is_connected():
            cursor.close()
            cnx.close()



def dellall():
    cnx = mysql.connector.connect(user='almogabudi',
                                    password='almogabu14007',
                                    host='localhost',
                                    database='almogabudi',
                                    port=3305)    
    cursor = cnx.cursor()

    # For MySQL or MariaDB
    query_list = [
             """DROP TABLE IF EXISTS movie_to_genres;""",
             """DROP TABLE IF EXISTS genres;""",
             """DROP TABLE IF EXISTS ratings;""",
             """DROP TABLE IF EXISTS keywords;""",
             """DROP TABLE IF EXISTS movie_to_companies;""",
             """DROP TABLE IF EXISTS production_companies;""",
             """DROP TABLE IF EXISTS movies_metadata;"""]
    for query in query_list:
        try:
            cursor.execute(query)
        except errorcode as e:
            print(f"Error: {e}")
    cursor.close()


if __name__ == "__main__":
    dellall()