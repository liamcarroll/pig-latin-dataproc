DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage;

movies_raw = LOAD 'gs://ca40220-movielens-data/data_raw/movies.csv' USING CSVExcelStorage() AS (movieId: int, title: chararray, genres: chararray);
movies_clean = FOREACH movies_raw GENERATE movieId, SUBSTRING($1, 0,(int)SIZE($1) - 7) AS title, REGEX_EXTRACT($1, '([0-9][0-9][0-9][0-9])', 1) AS year,  genres;

links_raw = LOAD 'gs://ca40220-movielens-data/data_raw/links.csv' USING CSVExcelStorage() AS (movieId: int, imdbId: int, tmdbId: int);

joined = JOIN movies_clean BY movieId, links_raw BY movieId;
movies_final = FOREACH joined GENERATE $0 AS movieId, $1 AS title, $2 AS year, $5 AS imdbId, $6 AS tmdbId, $3 AS genres;
STORE movies_final INTO 'gs://ca40220-movielens-data/data_processed/movies.csv' USING CSVExcelStorage();