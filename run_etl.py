# reddit NLP ETL prototype

# This code prototypes a Spark workflow that retrieves data sets of compressed reddit posts
# and outputs TF-IDF scores for each post. After processing, the records can be stored in
# an HDFS instance, an S3 bucket, or a local file system (for small jobs and testing).

# Imports

import argparse
from pyspark.sql import SparkSession
import time

from reddit_etl.data_retrieval.remote import get_file, unzip_bz2_file_to_text
import reddit_etl.data_analysis.nlp_pipeline as nlp
from reddit_etl.data_storage.block_level import write_spark_dataframe_to_file

## Main Function

  # Main function for prototype.
  # Generates a list of URLs for the provided url_stub and range of years.
  # Then uses a loop to retrieve each file, decompress it,and generate an RDD.
  # The union of these RDDs generates the DataFrame, and the NLP pipeline
  # outputs TF-IDF scores for each post to a new column.

if __name__ == '__main__':
  
  # Get URL list from args
  # Optionally, get local file path to save output

  parser = argparse.ArgumentParser()
  parser.add_argument("url_list", help="text file of URLs to call, one URL per line")
  parser.add_argument("-l", "--local", help="local storage path")
  args = parser.parse_args()

  # Start Spark session
  spark = SparkSession \
    .builder \
    .appName("RedditNlpEtl") \
    .getOrCreate()
  
  sc = spark.sparkContext
  
  # Read list of URLS from file and make list
  with open(args.url_list, 'rt') as f:
    reddit_urls = [line for line in f.read()]
  
  rdd_list = []

  # Display starting time (current time)
  print(f'Main loop starting at', {time.asctime()}, '\n')
  for url in reddit_urls:

    # Get file from website and decompress it to text  
    print(f'Now processing', url)

    reddit_file = get_file(url)
    unzipped_file = unzip_bz2_file_to_text(reddit_file)

    # Make an RDD from the file and append it to a list
    print('Generating RDD from file and appending to rdd_list\n')
    rdd_list.append(sc.parallelize(unzipped_file.splitlines()))

  
  # Create Spark DataFrame from RDDs
  print(f'Creating Spark DataFrame\n')
  reddit_df = spark.read.json(sc.union(rdd_list))

  # NLP pipeline
  print('Beginning NLP pipeline')
  print(f'\t* Tokenizing posts')
  reddit_tokenized_df = nlp.tokenize(reddit_df)
  print(f'\t* Removing stop words')
  reddit_filtered_df = nlp.remove_stopwords(reddit_tokenized_df)
  print(f'\t* Hashing posts')
  reddit_hashed_df = nlp.hasher(reddit_filtered_df)
  print(f'\t* Calculating TF-IDF scores')
  reddit_tfidf_df = nlp.tfidf_calc(reddit_hashed_df)

  # Display concluding time
  print(f'\nScript concluded at', {time.asctime()})

  # Store data locally for testing
  if args.local:
    write_spark_dataframe_to_file(reddit_tfidf_df, args.local)