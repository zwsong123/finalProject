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


def main(spark, model_file, test_file, tag_file, index_file):
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
    tag_df = spark.read.parquet(tag_file)
    index_file = spark.read.parquet(index_file)
    #df.show(50)
    print('ck1!')
    
    #label = df.select(['user_index','track_index']).groupBy("user_index").agg(f.collect_list('track_index').alias('actual track')).rdd
    #label.show(50)
    df.createOrReplaceTempView('my_table')

    listen = spark.sql('select track_index, count(track_index) as num_lis from my_table group by track_index order by num_lis')
    listen.createOrReplaceTempView('listen_table')
   
    print('ck2!')
    target = spark.sql('select distinct user_index from my_table')
    
    pred = model.recommendForUserSubset(target,10)
    #pred = pred.select(['user_index', 'recommendations.track_index'])
    
    print('ck3!')
    pred.createOrReplaceTempView('my_table_2')
    
    #nshow = spark.sql('select * from my_table_2 limit 3')
    #nshow.show(1)
    #nuser = spark.sql('select count(user_index) from my_table_2')
    #nuser.show()
    pred1 = spark.sql('select user_index, recommendations.track_index as pretrack from my_table_2')
    
    
    #pred_label = pred.join(label).map(lambda x: (x[1]))
    
    
    print('ck4!')
    list1 = []
    for row in pred1.rdd.collect():
        list1.extend(row.pretrack)
    print('ck5!')    
    rec = list1.toDF('re_track')
    rec.createOrReplaceTempView('rec_table')
    table1 = spark.sql('select rec_table.re_track as track_id, count(rec_table.re_track) as num_rec, listen_table.num_listen\
                       from rec_table inner join listen_table group by rec_table.re_track order by num_rec DESC')
    
    table1.show(5)
    
    
    
    
                                                                              
                                                                             
    
   
    
    
   
            
    
    
    
   
    

    
    
   
    


# Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('part1').getOrCreate()

    # Get the filename from the command line
    model_file = sys.argv[1]
    test_file = sys.argv[2]
    tag_file = sys.argv[3]
    index_file = sys.argv[4]

    # Call our main routine
    main(spark, model_file, test_file, tag_file, index_file)
