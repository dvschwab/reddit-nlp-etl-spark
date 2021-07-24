# reddit NLP ETL prototype

# This code prototypes a Spark workflow that retrieves data sets of compressed reddit posts
# and outputs TF-IDF scores for each post. After processing, a sample of records is exported
# to Amazon's DynamoDB for further analysis by a data science team.

# Initialize Spark instance

import findspark
findspark.init()

# Imports

import argparse
from pyspark.sql import SparkSession
import time

from get_posts import get_file, unzip_file
from nlp_pipeline import tokenize, remove_stopwords, hasher, tfidf_calc

## Main Function

  # Main function for prototype.
  # Generates a list of URLs for the provided url_stub and range of years.
  # Then uses a loop to retrieve each file, decompress it,and generate an RDD.
  # The union of these RDDs generates the DataFrame, and the NLP pipeline
  # outputs TF-IDF scores for each post to a new column.

if __name__ == '__main__':

  # Start Spark session

  spark = SparkSession.builder.appName("RedditNlpEtl").getOrCreate()
  sc = spark.sparkContext

  # Get start and end year from user args

  parser = argparse.ArgumentParser()
  parser.add_argument("url_list", help="text file of URLs to call, one URL per line")
  args = parser.parse_args()
  
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
    unzipped_file = unzip_file(reddit_file)

    # Make an RDD from the file and append it to a list
    print('Generating RDD from file and appending to rdd_list\n')
    rdd_list.append(sc.parallelize(unzipped_file.splitlines()))

  
  # Create Spark DataFrame from RDDs
  print(f'Creating DataFrame\n')
  reddit_df = spark.read.json(sc.union(rdd_list))

  # NLP pipeline
  print('Beginning NLP pipeline')
  print(f'\t* Tokenizing posts')
  reddit_tokenized_df = tokenize(reddit_df)
  print(f'\t* Removing stop words')
  reddit_filtered_df = remove_stopwords(reddit_tokenized_df)
  print(f'\t* Hashing posts')
  reddit_hashed_df = hasher(reddit_filtered_df)
  print(f'\t* Calculating TF-IDF scores')
  reddit_tfidf_df = tfidf_calc(reddit_hashed_df)

  # Display concluding time
  print(f'\nScript concluded at', {time.asctime()})