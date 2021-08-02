# reddit NLP ETL prototype

# This code prototypes a Spark workflow that retrieves data sets of compressed reddit posts
# and outputs TF-IDF scores for each post. After processing, a sample of records is exported
# to Amazon's DynamoDB for further analysis by a data science team.

# Initialize Spark instance
# Commented out while testing on local machine

""" import findspark
findspark.init() """

# Imports

""" from pyspark.sql import SparkSession
 """
# File handling and NLP

""" from pyspark import SparkFiles
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, StopWordsRemover """

# requests used to retrieve files
# bz2 to unzip them (bzip2 compression)
# time calculates running time of main loop

import requests
import bz2
import time

# Start Spark session

""" spark = SparkSession.builder.appName("RedditNlpEtlPoCTest").getOrCreate()
 """
## Function definitions

### Data Retrieval and Initial Processing

def generate_urls(url_stub, start_year, end_year):
  """
  Generates a list of URLs corresponding the the Reddit posts
  to be retrieved. The URL pattern is somewhat idiosyncratic,
  so if it changes this code will need to be modified.

  Arguments:
    url_stub: the common prefix used for all URLs
    start_year, end_year: the beginning and ending years
    of posts to retrieve. For a single year, set
    start_year = end_year. Both start_year and end_year
    are inclusive, so (2010, 2012) will return three years
    of URLs

  Returns:
    list of URLs with one URL for each month and year
    to be retrieved

  """
  
  # URLs appended to this list
  file_urls = []
  
  for year in range(start_year, (end_year + 1)):
    
    # Set extension by year for most files
      
    if year < 2018:
      extension = '.bz2'
    elif year < 2019:
      extension = '.xz'
    else:
      extension = '.zst'
      
    # Generate the URLs based on the observed patterns

    for month in range(1,13):
          
      # Handle a few special cases    
      if (year == 2017 and month == 12):
        extension = '.xz'
      if (year == 2018 and month in [11,12]):
        extension = '.zst'

      # Create the file name, adding the leading zero
      # if the month is 1 - 9
      if month < 10:
        file = 'RC_' + str(year) + '-0' + str(month) + extension
      else:
        file = 'RC_' + str(year) + '-' + str(month) + extension
              
      file_urls.append(url_stub + file)

  return file_urls

def get_file(url):
  """
    Retrieves a file from the supplied URL using the requests library.
    Basically a wrapper for requests.get(), but abstracted as a function
    so type-checking and error-handling can be added if needed.

    Argument:
      url: the URL specifying the resource to be retrieved

    Returns:
      The content of the request response. In this invocation, this
      will be the compressed file of reddit posts at the supplied URL

  """
  
  response = requests.get(url)

  return response.content

def unzip_file(zipped_file):
  """
    Decompresses the supplied file, which must be in bzip2 format.
    Requires the bz2 library.

    Argument:
      The compressed file in bzip2 format

    Returns:
      A (potentially very long) string containing the uncompressed
      file contents.
  """

  unzipped_file = bz2.decompress(zipped_file).decode()

  return unzipped_file

### NLP Pipeline Functions

def tokenize(nlp_df):
    
  nlp_tokenizer = Tokenizer(inputCol="body", outputCol="post_words")
  tokenized_df = nlp_tokenizer.transform(nlp_df)
  return tokenized_df

def remove_stopwords(tokenized_df):

  remover = StopWordsRemover(inputCol='post_words', outputCol='post_filtered')
  remover.loadDefaultStopWords('english')
  filtered_df = remover.transform(tokenized_df)
  return filtered_df

def hasher(hashable_df):

# Number of Features is default (262,144)

  hasher = HashingTF(inputCol='post_filtered', outputCol='post_hashed')
  hashed_df = hasher.transform(hashable_df)
  return hashed_df

def tfidf_calc(hashed_df):

  tfidf = IDF(inputCol='post_hashed', outputCol='post_tfidf')
  tfidfModel = tfidf.fit(hashed_df)
  tfidf_df = tfidfModel.transform(hashed_df)
  return tfidf_df

## Main Function

# This function retrieves the reddit posts, decompresses them, and constructs a Spark DataFrame from them.
# The DataFrame is then processed by the NLP pipeline to output estimated TF-IDF scores for each post.

# The corpus for this pipeline is the dowloaded posts: if posts from 2006 are downloaded,
# then the TF-IDF scores will be relative to 2006 posts. If 2006 and 2007 are downloaded,
# the scores will be relative to both years; and so on.


if __name__ == '__main__':

  # Main function for prototype.
  # Generates a list of URLs for the provided url_stub and range of years.
  # Then uses a loop to retrieve each file, decompress it,
  # and generate an RDD.
  # The union of these RDDs generates the DataFrame,
  # and the NLP pipeline outputs TF-IDF scores for each post
  # to a new column.

  start_year = 2006
  end_year = 2006
  url_stub = 'https://files.pushshift.io/reddit/comments/'
  reddit_urls = generate_urls(url_stub, start_year, end_year)

  """sc = spark.sparkContext"""

  rdd_list = []

  # Display starting time (current time)
  print(f'Main loop starting at', {time.asctime()}, '\n')
  for url in reddit_urls:

    # Get file from website and decompress it to text  
    file_name = url.split('/')[5].split('.')[0]
    print(f'Now processing', file_name)

    reddit_file = get_file(url)
    unzipped_file = unzip_file(reddit_file)
  
    # Not saving files during this test run
    # s3.Bucket(s3_bucket_prefix).put_object(Key=file_name, Body=unzipped_file)

    # Make an RDD from the file and append it to a list
    print('Generating RDD from file and appending to rdd_list\n')
    rdd_list.append(sc.parallelize(unzipped_file.splitlines()))

  
  # Create Spark DataFrame from RDDs
  # (easier than appending each one in succession;
  # not sure if faster or not)
  print(f'Creating DataFrame of posts from years', start_year, 'to', end_year, '\n')
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