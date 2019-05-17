#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''A simple pyspark script to count the number of rows in a parquet-backed dataframe
Usage:
    $ spark-submit count.py hdfs:/path/to/file.parquet
'''


# We need sys to get the command line arguments
import sys

# And pyspark.sql to get the spark session
import sys
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.mllib.evaluation import RankingMetrics
import pyspark.sql.functions as f
sc = SparkContext()


def main(spark, modelname, filename):
    '''Main routine for the row counter
    Parameters
    ----------
    spark : SparkSession object
    filename : string, path to the parquet file to load
    '''

    model = ALSModel.load(modelname)
    df = spark.read.parquet(filename).select(['user_index', 'track_index', 'count']).orderBy(['user_index', 'count'], ascending = False)

    # Give the dataframe a temporary view so we can run SQL queries
    df.createOrReplaceTempView('my_table')

    # Construct a query
    query = spark.sql('SELECT * FROM my_table')

    # Print the results to the console
    query.show(50)


# Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('part1').getOrCreate()

    # Get the filename from the command line
    filename = sys.argv[1]

    # Call our main routine
    main(spark, filename)
