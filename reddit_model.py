""" reddit_model.py
This module implements a Reddit Politics Sentiment Classifier using Spark mllib.

Date Created:
    May 28th, 2019
"""

# ---------------------------------------------------------------------------- #
# Import Statements for the Necessary Packages
# ---------------------------------------------------------------------------- #
from __future__ import print_function
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf # Spark User Defined Function
from pyspark.sql.types import ArrayType, StringType # UDF Return Types
from cleantext import sanitize # Tokenizer
import pandas as pd # Pandas DataFrame Printing

# ---------------------------------------------------------------------------- #
# Main Function
# ---------------------------------------------------------------------------- #
def main(context):
    """Main function takes a Spark SQL context."""
    #---------------------------------------------------------------------------
    # TASK 1
    # df = context.read.csv('labeled_data.csv')
    # df.write.parquet("labeled_data.parquet")
    # comments = context.read.json("comments-minimal.json.bz2") 
    # comments.write.parquet("comments.parquet")
    # submissions = context.read.json("submissions.json.bz2")
    # submissions.write.parquet("submissions.parquet")
    labeled_data = context.read.parquet('labeled_data.parquet')
    # labeled_data.show()
    comments = context.read.parquet('comments.parquet')
    # comments.show()
    submissions = context.read.parquet('submissions.parquet')
    # submissions.show()

    #---------------------------------------------------------------------------
    # TASK 4
    sanitize_udf = udf(sanitize, ArrayType(StringType()))

    #---------------------------------------------------------------------------
    # TASK 5
    sanitized = comments.select(sanitize_udf('body').alias('body_sanitize'))
    df = sanitized.limit(10).toPandas() # Pretty Printing Only
    print(df) # Pretty Printing Only

if __name__ == "__main__":
    conf = SparkConf().setAppName("CS143 Project 2B")
    conf = conf.setMaster("local[*]")
    sc   = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    sc.addPyFile("cleantext.py")
    main(sqlContext)