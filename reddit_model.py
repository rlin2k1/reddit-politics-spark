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

from pyspark.ml.classification import LogisticRegression
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder 
from pyspark.ml.evaluation import BinaryClassificationEvaluator

from pyspark.ml.feature import CountVectorizer

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
    sanitized = comments.select(sanitize_udf('body').alias('raw'))
    df = sanitized.limit(10).toPandas() # Pretty Printing Only
    print(df) # Pretty Printing Only

    #---------------------------------------------------------------------------
    # TASK 6A
    cv = CountVectorizer(binary=True, minDF=10.0, inputCol="raw", outputCol="vectors")
    model = cv.fit(sanitized)
    model.transform(sanitized).show(truncate=False)
    countVectorizerPath = "count_vectorizer"
    cv.save(countVectorizerPath)

    #---------------------------------------------------------------------------
    # TASK 6B

    #---------------------------------------------------------------------------
    # TASK 7: MACHINE LEARNING PORTION TO TRAIN MODELS
    # Initialize two logistic regression models.
    # Replace labelCol with the column containing the label, and featuresCol with the column containing the features.
    poslr = LogisticRegression(labelCol="poslabel", featuresCol="features", maxIter=10)
    neglr = LogisticRegression(labelCol="neglabel", featuresCol="features", maxIter=10)
    # This is a binary classifier so we need an evaluator that knows how to deal with binary classifiers.
    posEvaluator = BinaryClassificationEvaluator()
    negEvaluator = BinaryClassificationEvaluator()
    # There are a few parameters associated with logistic regression. We do know what they are a priori.
    # We do a grid search to find the best parameters. We can replace [1.0] with a list of values to try.
    # We will assume the parameter is 1.0. Grid search takes forever. 
    posParamGrid = ParamGridBuilder().addGrid(poslr.regParam, [1.0]).build()
    negParamGrid = ParamGridBuilder().addGrid(neglr.regParam, [1.0]).build()
    # We initialize a 5 fold cross-validation pipeline.
    posCrossval = CrossValidator(estimator=poslr, evaluator=posEvaluator, estimatorParamMaps=posParamGrid, numFolds=5)
    negCrossval = CrossValidator(estimator=neglr, evaluator=negEvaluator, estimatorParamMaps=negParamGrid, numFolds=5)
    # Although crossvalidation creates its own train/test sets for # tuning, we still need a labeled test set, because it is not # accessible from the crossvalidator (argh!)
    # Split the data 50/50
    posTrain, posTest = pos.randomSplit([0.5, 0.5]) 
    negTrain, negTest = neg.randomSplit([0.5, 0.5]) 
    # Train the models
    print("Training positive classifier...") 
    posModel = posCrossval.fit(posTrain) 
    print("Training negative classifier...")
    negModel = negCrossval.fit(negTrain)
    # Once we train the models, we don't want to do it again. We can save the models and load them again later.
    posModel.save("project2/pos.model")
    negModel.save("project2/neg.model")
    

if __name__ == "__main__":
    conf = SparkConf().setAppName("CS143 Project 2B")
    conf = conf.setMaster("local[*]")
    sc   = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    sc.addPyFile("cleantext.py")
    main(sqlContext)