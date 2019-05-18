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
from pyspark.sql.types import IntegerType
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
    
    index_df = spark.read.parquet(index_file)
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
    #for row in pred1.rdd.collect():
     #   list1.extend(row.pretrack)
    print('ck5!')    
    
   
    list2 = [1,2,3,4,5,33,1,2,5,5]
    
    
    rec = spark.createDataFrame(list2, IntegerType())
    rec.createOrReplaceTempView('rec_table')
    rec.show()
    
    table1 = spark.sql('select rec_table.value as track_id, count(rec_table.value) as num_rec\
                       from rec_table group by rec_table.value order by num_rec DESC')
                       
    
    table1.createOrReplaceTempView('rec_table_2')
             
    table2 = sparl.sql('select rec_table_2.track_id, ifnull(rec_table_2.num_rec,0), ifnull(listen_table.num_lis,0) from rec_table \
                          full outer join listen_table on rec_table.track_id = listen_table.track_index')
  
    
    print('ck6!')
    
    table2['score'] = table2['num_rec'] - table2['num_lis']
    table2.show(10)
    
    #tag_df.createOrReplaceTempView('tag_table')
    #index_df.createOrReplaceTempView('index_table') 
    #table3 = spark.sql('select main_table.track_id, main_table.score, tag_table.tags from main_table left join index_table on\
                        #main_table.track_id = index_table.xxxxx left join tag_table on index_table.yyyy = tag_table.track_id')
    
    
    
    
    
    
                                                                              
                                                                             
    
   
    
    
   
            
    
    
    
   
    

    
    
   
    


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
