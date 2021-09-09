# reddit-nlp-etl-spark
Using Spark EMR on AWS, calculates TF-IDF scores for reddit posts for the past several years and then stores the output in Amazon's NoSQL DynamoDB. The Spark cluster has 12 nodes with a total of 48 cores and 192G of RAM. The total data set includes approximately 2.5B records.

Currently, a working prototype has been produced and tested for years 2006 and 2007 (approximately 500,000 posts) on Google Colab using their time-share Spark instance. The prototype is in the process of being converted to a stand-alone Python module and tested on a 5 node Spark cluster with a reduced data set. A full test of the complete data set is scheduled for the first week of August.

## Update 8/2/2021
Prototype code has been refactored to the Python module reddit_etl containing the submodules *credentials*, *data_retrieval*, *data_analysis*, and *data_storage* and unit testing has begun. The Docker Spark image from bitnami is being used for the tests involving Spark itself.

To facillitate storing the calculated TF-IDF scores in HBase, the Spark cluster has been reduced to 8 nodes and a 4 node Hadoop cluster has been provisioned on AWS. This is because the AWS EMR service doesn't have Spark and HBase in the same instance.

## Update 8/11/2021
Code has passed initial tests on Docker spark instance. Now building a multi-container app with a Spark gateway (i.e. ubuntu server), Spark, and HDFS for more extensive local testing before attempting deployment to AWS.
