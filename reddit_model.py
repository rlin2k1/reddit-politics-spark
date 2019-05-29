from __future__ import print_function
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import pandas


def main(context):
    """Main function takes a Spark SQL context."""
    # YOUR CODE HERE
    # YOU MAY ADD OTHER FUNCTIONS AS NEEDED

    #---------------------------------------------------------------------------
    # TASK 1
    df = context.read.csv('labeled_data.csv')
    df.write.parquet("labeled_data.parquet")
    comments = context.read.json("comments-minimal.json.bz2") 
    df.write.parquet("comments.parquet")
    submissions = context.read.json("submissions.json.bz2")
    df.write.parquet("submissions.parquet")

if __name__ == "__main__":
    conf = SparkConf().setAppName("CS143 Project 2B")
    conf = conf.setMaster("local[*]")
    sc   = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    sc.addPyFile("cleantext.py")
    main(sqlContext)