DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage;

ratings_raw = LOAD 'gs://ca40220-movielens-data/data_raw/ratings.csv' USING CSVExcelStorage() AS (userId: int, movieId: int, rating: float, timestamp: int);

grouped_user = GROUP ratings_raw BY userId;
avg_ratings = FOREACH grouped_user GENERATE group, AVG(ratings_raw.rating) AS ratings_avg, COUNT(ratings_raw.rating) AS ratings_cnt;
sorted_users = ORDER avg_ratings BY ratings_avg DESC, ratings_cnt DESC;
out = LIMIT sorted_users 10;
DUMP out;