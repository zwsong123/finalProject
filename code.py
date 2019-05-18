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
    #read model and file
    model = ALSModel.load(model_file)
    df = spark.read.parquet(test_file).select(['user_index', 'track_index', 'count'])\
                     .orderBy(['user_index', 'count'], ascending = False)
    #df.show(50)
    
    #label = df.select(['user_index','track_index']).groupBy("user_index").agg(f.collect_list('track_index').alias('actual track')).rdd
    #label.show(50)
    df.createOrReplaceTempView('my_table')

    listen = spark.sql('select track_index, count(track_index) as ucount from my_table group by track_index order by ucount')
    listen.show(50)
    
    pred = model.recommendForAllUsers(20)
    pred = pred.select(['user_index', 'recommendations.track_index'])
    
    pred.createOrReplaceTempView('my_table_2')
    
    nshow = spark.sql('select * from my_table_2 limit 3')
    nshow.show(1)
    nuser = spark.sql('select count(user_index) from my_table_2')
    nuser.show()

    
    #pred_label = pred.join(label).map(lambda x: (x[1]))
    
    list1 = []
    for row in pred.rdd.collect():
        list1.extend(row.recommendations.track_index)
    
    
    #overr = label.map(lambda x: x[0]-x[1])
    #underr = label.map(lambda x: x[1]-x[0])
    
    #score = overr.select('track_index',count('track_index')).groupby('track_index')
                                                                              
                                                                             
    
   
    
    
   
            
    
    
    
   
    

    
    
   
    


# Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('part1').getOrCreate()

    # Get the filename from the command line
    model_file = sys.argv[1]
    test_file = sys.argv[2]

    # Call our main routine
    main(spark, model_file, test_file)
