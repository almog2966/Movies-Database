import pandas as pd
from pathlib import Path
import ast


COLOUMNS_METADATA = ['adult', 'budget', 'genres','id','original_language',
                   'overview','popularity','production_companies','release_date',
                   'revenue','runtime','tagline','title']
COLUMNS_RATINGS = ['movieId','rating']
def is_id_less_than(id_str):
    try:
        return int(id_str) < 1000
    except ValueError:
        # Return False if conversion fails
        return False
    
def readCSV():
    # Realtive Path to the csv files
    csv_path = Path(__file__).parent.parent.parent / 'tables' / 'input_tables'
    # Read the original CSV file into a DataFrame
    movies_metadata = pd.read_csv(csv_path / 'movies_metadata.csv', usecols=COLOUMNS_METADATA, low_memory=False)
    movies_metadata['popularity'] = pd.to_numeric(movies_metadata['popularity'], errors='coerce')

    keywords = pd.read_csv(csv_path / 'keywords.csv')
    ratings = pd.read_csv(csv_path / 'ratings.csv',usecols=COLUMNS_RATINGS)
    ratings.rename(columns={'movieId': 'id'}, inplace=True)
    
    return movies_metadata,keywords,ratings

def filterDF(movies_metadata,keywords,ratings):
    # Filter the moviess id less than 1000
    movies_metadata = movies_metadata[movies_metadata['id'].apply(is_id_less_than)]
    movies_metadata.loc[:, 'genres'] = movies_metadata['genres'].apply(ast.literal_eval)
    movies_metadata.loc[:, 'production_companies'] = movies_metadata['production_companies'].apply(ast.literal_eval)
    movies_metadata.loc[:, 'id'] = movies_metadata['id'].astype('int64')
    
    # Filter the keyword who only in movies meta data and foramt the key words, sord by id
    df_keyword = keywords[keywords['id'].isin(movies_metadata['id'])]
    df_keyword.loc[:, 'keywords'] = df_keyword['keywords'].apply(ast.literal_eval)
    df_keyword.loc[:, 'keywords'] = df_keyword['keywords'].apply(lambda x: ', '.join([entry['name'] for entry in x]))

    # AVG rating per movie in movie meta data , sorted rating descending
    df_ratings = ratings[ratings['id'].isin(movies_metadata['id'])]
    df_ratings = df_ratings.groupby('id')['rating'].mean().reset_index().sort_values(by='rating', ascending=False)

    return movies_metadata,df_keyword,df_ratings


def createCSV(movies_metadata,keywords,ratings,df_genres,df_movie_to_genre,df_production,df_production_to_movie):
    # Remove the genre and production and create new CSV file
    df_metadata = movies_metadata[movies_metadata['id'].apply(is_id_less_than)]
    df_metadata.drop(['genres','production_companies'], axis = 1, inplace = True)
    csv_path = Path(__file__).parent.parent.parent / 'tables' / 'output_tables'

    # Save csv files
    df_metadata.to_csv(csv_path / 'movies metadata.csv', index=False)
    keywords.to_csv(csv_path / 'keywords.csv', index=False)
    ratings.to_csv(csv_path / 'ratings.csv', index=False)
    df_genres.to_csv(csv_path / 'genres.csv', index=False)
    df_movie_to_genre.to_csv(csv_path / 'movie-genre.csv', index=False)
    df_production.to_csv(csv_path / 'production-companies.csv', index=False)
    df_production_to_movie.to_csv(csv_path / 'movie-company.csv', index=False)


def createGenreDF(df_metadata):
    # Create new DF of all gernes and ids
    genres = pd.DataFrame({'genre id':[], 'name':[]})
    all_genres = [genre for sublist in df_metadata['genres'] for genre in sublist]
    df_genres = pd.DataFrame(all_genres).drop_duplicates().reset_index(drop=True)
    df_genres = df_genres.rename(columns={'id': 'genre id'})
    df_genres = df_genres.sort_values(by='name')
    
    return df_genres

def createMovieToGenre(df_metadata):

    movie_genre_mappings = []

    for index, row in df_metadata.iterrows():
        movie_id = row['id']
        for genre in row['genres']:
            movie_genre_mappings.append({'movie_id': movie_id, 'genre_id': genre['id']})
            
    movie_genres_df = pd.DataFrame(movie_genre_mappings)
    movie_genres_df = movie_genres_df.sort_values(by='movie_id')
    return movie_genres_df


def createProdcutionTable(movies_metadata):
    # create df of all companies name and id
    all_companies = [company for sublist in movies_metadata['production_companies'] for company in sublist]
    companies_df = pd.DataFrame(all_companies).drop_duplicates().reset_index(drop=True)
    companies_df = companies_df.rename(columns={'id': 'company id'})
    companies_df = companies_df.sort_values(by='name')
    return companies_df


def createProductionToMovie(movies_metadata):

    movie_company_mappings = []

    for index, row in movies_metadata.iterrows():
        movie_id = row['id']
        for company in row['production_companies']:
            movie_company_mappings.append({'movie_id': movie_id, 'company_id': company['id']})
            
    movie_company_df = pd.DataFrame(movie_company_mappings)
    movie_company_df = movie_company_df.sort_values(by='movie_id')
    return movie_company_df




def main():
    movies_metadata,keywords,ratings = readCSV()
    movies_metadata,keywords,ratings = filterDF(movies_metadata,keywords,ratings)
    df_genres = createGenreDF(movies_metadata)
    df_movie_to_genre = createMovieToGenre(movies_metadata)
    df_production = createProdcutionTable(movies_metadata)
    df_production_to_movie = createProductionToMovie(movies_metadata)
    createCSV(movies_metadata,keywords,ratings,df_genres,df_movie_to_genre,df_production,df_production_to_movie)
    print("make csv done!")


if __name__ == "__main__":
    main()

