DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage;

ratings_raw = LOAD 'gs://ca40220-movielens-data/data_raw/ratings.csv' USING CSVExcelStorage() AS (userId: int, movieId: int, rating: float, timestamp: int);
movies_final = LOAD 'gs://ca40220-movielens-data/data_processed/movies.csv' USING CSVExcelStorage() AS (movieId: int, title: chararray, year: int, imdbId: int, tmdbId: int,genres: chararray);

grouped_movies = GROUP ratings_raw BY movieId;
count_ratings = FOREACH grouped_movies GENERATE group, COUNT(ratings_raw) AS ratings_cnt;

top_rated_movies = JOIN count_ratings BY group, movies_final BY movieId;
sorted = ORDER top_rated_movies BY ratings_cnt DESC;
out = LIMIT sorted 10;
DUMP out;