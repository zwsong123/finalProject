#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Part 1: supervised model testing
Usage:
    $ spark-submit supervised_test.py hdfs:/path/to/load/model.parquet hdfs:/path/to/file
'''


# We need sys to get the command line arguments
import sys

# And pyspark.sql to get the spark session
from pyspark.sql import SparkSession

# TODO: you may need to add imports here
from pyspark.ml import PipelineModel
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.ml.feature import SQLTransformer
from pyspark import SparkContext
from pyspark import SparkConf 


def main(spark, model_file, data_file):
    '''Main routine for supervised evaluation
    Parameters
    ----------
    spark : SparkSession object
    model_file : string, path to store the serialized model file
    data_file : string, path to the parquet file to load
    '''

    ###
    # TODO: YOUR CODE GOES HERE
    
    df = spark.read.parquet(data_file)
    
    model = PipelineModel.load(model_file)
    predicts = model.transform(df)
    
    
    sc = SparkContext.getOrCreate()
    
    predictionAndLabels = predicts.rdd.map(lambda x: (x.prediction, x.label))
    #predictionAndLabels=df.rdd.map(lambda lp: (float(model.predict(lp.features)), lp.label))
    
    # Instantiate metrics object
    metrics = MulticlassMetrics(predictionAndLabels)

    # Overall statistics
    precision = metrics.precision()
    recall = metrics.recall()
    f1Score = metrics.fMeasure()
    print("Summary Stats")
    print("Precision = %s" % precision)
    print("Recall = %s" % recall)
    print("F1 Score = %s" % f1Score)

    # Statistics by class
    labels = predicts.rdd.map(lambda x: x.label).distinct().collect()
    for label in sorted(labels):
        print("Class %s precision = %s" % (label, metrics.precision(label)))
        print("Class %s recall = %s" % (label, metrics.recall(label)))
        print("Class %s F1 Measure = %s" % (label, metrics.fMeasure(label, beta=1.0)))

    # Weighted stats
    print("Weighted recall = %s" % metrics.weightedRecall)
    print("Weighted precision = %s" % metrics.weightedPrecision)
    print("Weighted F(1) Score = %s" % metrics.weightedFMeasure())

    
    
    
    ###

    pass




# Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('supervised_test').getOrCreate()

    # And the location to store the trained model
    model_file = sys.argv[1]

    # Get the filename from the command line
    data_file = sys.argv[2]

    # Call our main routine
    main(spark, model_file, data_file)
