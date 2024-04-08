import mysql.connector
from mysql.connector import Error

def query_1(cursor,keyword):
    # Search by keyword for top 5 rated movies using FULLTEXT search.
    try:
        sql = """
        SELECT m.title, m.tagline, r.rating
        FROM movies_metadata m
        LEFT JOIN keywords k ON m.movie_id = k.movie_id
        LEFT JOIN ratings r ON m.movie_id = r.movie_id
        WHERE MATCH(k.keywords) AGAINST(%s IN NATURAL LANGUAGE MODE)
        ORDER BY r.rating DESC
        LIMIT 5;
        """
        cursor.execute(sql,(keyword,))
        return cursor.fetchall()
    except Error as e:
        print(f"An Error has occurred: {e}")


def query_2(cursor,companyName):
    # Search company by text and return number of movies produced and AVG popularity
    try:
        sql = """
        SELECT pc.name AS CompanyName, COUNT(m.movie_id) AS NumberOfMovies, AVG(m.popularity) AS AvgPopularity
        FROM production_companies pc
        LEFT JOIN movie_to_companies mtc ON pc.company_id = mtc.company_id
        LEFT JOIN movies_metadata m ON mtc.movie_id = m.movie_id
        WHERE MATCH(pc.name) AGAINST(%s IN BOOLEAN MODE)
        GROUP BY pc.name
        ORDER BY AvgPopularity DESC;
        """
        cursor.execute(sql,(companyName,))
        return cursor.fetchall()
    except Error as e:
        print(f"An Error has occurred: {e}")

def query_3(cursor,overview_keyword,min_timerange,max_timerange):
# Search for family friendly movies by overeview and runtime range whos popularity is above AVG
    try:
        sql = """
        SELECT m.title, m.runtime, m.popularity
        FROM movies_metadata AS m
        WHERE MATCH(m.overview) AGAINST(%s IN NATURAL LANGUAGE MODE) AND
            m.runtime >= %s AND
            m.runtime <= %s AND
            m.adult = 'FALSE' AND
            m.popularity >= (
                SELECT AVG(popularity) FROM (
                    SELECT popularity
                    FROM movies_metadata
                ) as SUB
            )
        ORDER BY popularity DESC;
        """
        cursor.execute(sql,(overview_keyword,min_timerange,max_timerange))
        return cursor.fetchall()
    except Error as e:
        print(f"An Error has occurred: {e}")

def query_4(cursor,language):
    # Top 5 highest-rated movies per genre for inputed language.
    try:
        sql = """
        SELECT genre, title, rating
        FROM (
            SELECT g.name AS genre, m.title, r.rating as rating,
                ROW_NUMBER() OVER(PARTITION BY g.name ORDER BY r.rating DESC) as `rank`
            FROM movies_metadata m
            LEFT JOIN movie_to_genres mtg ON m.movie_id = mtg.movie_id
            LEFT JOIN genres g ON mtg.genre_id = g.genre_id
            LEFT JOIN ratings r ON m.movie_id = r.movie_id
            WHERE m.original_language = %s
        ) AS RankedMovies
        WHERE `rank` <= 5;
        """
        cursor.execute(sql,(language,))
        return cursor.fetchall()
    except Error as e:
        print(f"An Error has occurred: {e}")


def query_5(cursor,language):
    # Top 5 highest grossing movies in each genre for inputed language.
    try:
        sql = """
        SELECT genre, title, revenue
        FROM (
            SELECT g.name AS genre, m.title, m.revenue,
            ROW_NUMBER() OVER(PARTITION BY g.name ORDER BY m.revenue DESC) AS `rank`
            FROM movies_metadata m
            LEFT JOIN movie_to_genres mtg ON m.movie_id = mtg.movie_id
            LEFT JOIN genres g ON mtg.genre_id = g.genre_id
            WHERE m.revenue IS NOT NULL AND m.revenue > 0 AND 
                m.original_language = %s) AS TopGrossing
        WHERE `rank` <= 5
        ORDER BY genre;
        """
        cursor.execute(sql,(language,))
        return cursor.fetchall()
    except Error as e:
        print(f"An Error has occurred: {e}")

def query_6(cursor):
    # Find all movies that have been rated higher than 4.0 and have are multi genred.
    try:
        sql = """
        SELECT m.title, r.rating AS movie_rating
        FROM movies_metadata m
        LEFT JOIN ratings r ON m.movie_id = r.movie_id
        WHERE r.rating > 4.0
        AND EXISTS (
            SELECT 1
            FROM movie_to_genres mtg
            WHERE mtg.movie_id = m.movie_id
            GROUP BY mtg.movie_id
            HAVING COUNT(DISTINCT mtg.genre_id) >= 2
        )
        ORDER BY r.rating DESC;
        """
        cursor.execute(sql)
        return cursor.fetchall()
    except Error as e:
        print(f"An Error has occurred: {e}")

def query_test(cursor):
    # Find all movies that have been rated higher than 4.0 and have are multi genred.
    sql = """
    SELECT COUNT(*) FROM ratings;
    """
    cursor.execute(sql)
    return cursor.fetchall()
# Example usage:
if __name__ == "__main__":
    # Establish a database connection (example with sqlite3)
    cnx = mysql.connector.connect(user='almogabudi',
                                  password='almogabu14007',
                                  host='127.0.0.1',
                                  database='almogabudi',
                                  port=3305)
    cursor = cnx.cursor()


    # Example of how to call one of the functions
    rows = query_1(cursor,"Berlin")
    columns = [column[0] for column in cursor.description]
    print("\t".join(columns))  # Print column names
    for row in rows:
        print("\t".join(str(value) for value in row))

    # Close the connection
    cnx.close()
