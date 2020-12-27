DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage;

movies_final = LOAD 'gs://ca40220-movielens-data/data_processed/movies.csv' USING CSVExcelStorage();

out = LIMIT movies_final 10;
DUMP out;