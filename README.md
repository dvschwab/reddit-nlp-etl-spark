# reddit-nlp-etl-spark
Using Spark EMR on AWS, calculates TD-IDF scores for reddit posts for the past several years and then stores the output in Amazon's NoSQL DynamoDB. The Spark cluster has 12 nodes with a total of 48 cores and 192G of RAM. The total data set includes approximately 2.5B records.

Currently, a working prototype has been produced and tested for years 2006 and 2007 (approximately 500,000 posts) on Google Colab using their time-share Spark instance. The prototype is in the process of being converted to a stand-alone Python module and tested on a 5 node Spark cluster with a reduced data set. A full test of the complete data set is scheduled for the first week of August.
