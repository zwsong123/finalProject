#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''A simple pyspark script to count the number of rows in a parquet-backed dataframe
Usage:
    $ spark-submit count.py hdfs:/path/to/file.parquet
'''


# We need sys to get the command line arguments
import sys

# And pyspark.sql to get the spark session
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.mllib.evaluation import RankingMetrics
import pyspark.sql.functions as f
sc = SparkContext()


def main(spark, model_file, test_file):
    '''Main routine for the row counter
    Parameters
    ----------
    spark : SparkSession object
    filename : string, path to the parquet file to load
    '''

    model = ALSModel.load(model_file)
    
    df = spark.read.parquet(test_file).select(['user_index', 'track_index', 'count']).orderBy(['user_index', 'count'], ascending = False)
    df.createOrReplaceTempView('my_table')
    
    count = spark.sql('SELECT track_index, sum(count) as listened FROM my_table GROUPBY track_index ORDERBY listened')
    count.show(50)


    # Recommend top 100 tracks to each users
    pred = model.recommendForAllUsers(100)

    print(pred.schema)
    
    #pred = pred.select(['user_index', 'recommendations.track_index']).rdd
    #pred_label = pred.map(lambda x: (x[1]))
    
    
    
    
   
    


# Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('part1').getOrCreate()

    # Get the filename from the command line
    model_file = sys.argv[1]
    test_file = sys.argv[2]

    # Call our main routine
    main(spark, model_file, test_file)
