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
from pyspark.sql.functions import count, avg, sum, udf # Spark User Defined Function
# UDF Return Types
from pyspark.sql.types import ArrayType, StringType, IntegerType, BooleanType
from cleantext import sanitize # Tokenizer

from pyspark.ml.classification import LogisticRegression
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder, CrossValidatorModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.feature import CountVectorizerModel

import os
import re # Regex
import pandas as pd # Pandas DataFrame Printing

# ---------------------------------------------------------------------------- #
# Helper Functions (To Turn Into UDFs)
# ---------------------------------------------------------------------------- #
def get_index_0(vec):
    return str(vec[0])
def get_index_1(vec):
    return str(vec[1])

def predict_pos(vec):
    if float(vec[1]) > 0.2:
        return 1
    return 0

def predict_neg(vec):
    if float(vec[1]) > 0.25:
        return 1
    return 0

def strip_t3(text):
    """Strips first 3 characters of text."""
    return text[3:]

def sarcastic_or_quote(text):
    """Returns False if string contains '/s' or starts with &gt"""
    sarcastic_or_quote_match = re.compile(r'^&gt|\/s')
    return not sarcastic_or_quote_match.search(text)

states = ['Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', \
        'Connecticut', 'Delaware', 'District of Columbia', 'Florida', 'Georgia', 'Hawaii', \
        'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine', \
        'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 'Missouri', \
        'Montana', 'Nebraska', 'Nevada', 'New Hampshire', 'New Jersey', 'New Mexico', \
        'New York', 'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon', \
        'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota', 'Tennessee', \
        'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming']

def check_state(text):
    return text in states

# ---------------------------------------------------------------------------- #
# Main Function with Apache Spark SQL Context
# ---------------------------------------------------------------------------- #
def main(context):
    """Main Function takes a Spark SQL Context."""
    #---------------------------------------------------------------------------
    # TASK 1
    # Code for task 1...
    # df = context.read.csv('labeled_data.csv')
    # df.write.parquet("labeled_data.parquet")
    # comments = context.read.json("comments-minimal.json.bz2") 
    # comments.write.parquet("comments.parquet")
    # submissions = context.read.json("submissions.json.bz2")
    # submissions.write.parquet("submissions.parquet")
    labeled_data = context.read.parquet('labeled_data.parquet')
    labeled_data = labeled_data.withColumnRenamed("_c0", "Input_id")\
                               .withColumnRenamed("_c1", "labeldem")\
                               .withColumnRenamed("_c2", "labelgop")\
                               .withColumnRenamed("_c3", "labeldjt")
    # labeled_data.show()
    comments = context.read.parquet('comments.parquet')
    # comments.show()
    submissions = context.read.parquet('submissions.parquet')
    # submissions.show()

    #---------------------------------------------------------------------------
    # TASK 3
    # Code for task 3...
    labeled_comments = labeled_data.join(comments, comments.id == labeled_data.Input_id)
    labeled_comments = labeled_comments.select('Input_id', 'labeldjt', 'body')
    # labeled_comments.show()

    #---------------------------------------------------------------------------
    # TASK 4
    # Code for task 4...
    sanitize_udf = udf(sanitize, ArrayType(StringType()))
    
    #---------------------------------------------------------------------------
    # TASK 5
    # Code for task 5...
    sanitized_labeled_comments = labeled_comments.select('Input_id', 'labeldjt', sanitize_udf('body').alias('raw'))

    #---------------------------------------------------------------------------
    # TASK 6A
    # Code for task 6A...
    cv = CountVectorizer(binary=True, minDF=10.0, inputCol="raw", outputCol="features")
    model = cv.fit(sanitized_labeled_comments)
    sanitized_labeled_comments = model.transform(sanitized_labeled_comments)
    sanitized_labeled_comments.show(truncate=False)
    countVectorizerPath = "count_vectorizer_model"
    model.save(countVectorizerPath)

    #---------------------------------------------------------------------------
    # TASK 6B
    # Code for task 6B... - Labels: {1, 0, -1, -99}
    pos = sanitized_labeled_comments.select(sanitized_labeled_comments.features, sanitized_labeled_comments.labeldjt.cast(IntegerType()))
    pos = pos.withColumnRenamed("labeldjt", "label")
    pos = pos.replace(-1, 0)
    pos = pos.replace(-99, 0)
    # pos.show()

    neg = sanitized_labeled_comments.select(sanitized_labeled_comments.features, sanitized_labeled_comments.labeldjt.cast(IntegerType()))
    neg = neg.withColumnRenamed("labeldjt", "label")
    neg = neg.replace(1, 0)
    neg = neg.replace(-99, 0)
    neg = neg.replace(-1, 1)
    # neg.show()

    #---------------------------------------------------------------------------
    # TASK 7
    # Code for task 7... : MACHINE LEARNING PORTION TO TRAIN MODELS - Initialize two logistic regression models.
    # Replace labelCol with the column containing the label, and featuresCol with the column containing the features.
    poslr = LogisticRegression(labelCol="label", featuresCol="features", maxIter=10)
    neglr = LogisticRegression(labelCol="label", featuresCol="features", maxIter=10)
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

    # Positive Model: posModel
    # Negative Model: negModel
    
    #---------------------------------------------------------------------------
    # TASK 8
    # Code for task 8...: Make Final Deliverable for Unseen Data - We don't need labeled_data anymore
    strip_t3_udf = udf(strip_t3, StringType())
    sarcastic_or_quote_udf = udf(sarcastic_or_quote, BooleanType())
    # Get Unseen Data
    sanitized_final_deliverable = comments.select('created_utc', strip_t3_udf(comments.link_id).alias('link_id'), 'author_flair_text', 'id', 'body', sanitize_udf('body').alias('raw'), comments.score.alias('c_score'))\
        .filter(sarcastic_or_quote_udf(comments['body'])) #F.when(comments["body"].rlike('^&gt|\/s'), False).otherwise(True))
    # sanitized_final_deliverable.show()

    #---------------------------------------------------------------------------
    # TASK 9
    # Code for task 9...
    model = CountVectorizerModel.load("count_vectorizer_model") # TODO DELETE BEFORE SUBMITTING
    posModel = CrossValidatorModel.load("project2/pos.model") # TODO DELETE BEFORE SUBMITTING
    negModel = CrossValidatorModel.load("project2/neg.model") # TODO DELETE BEFORE SUBMITTING

    # Sanitize TASK 8 - Run the CountVectorizerModel on TASK 8 Relation
    sanitized_final_deliverable = model.transform(sanitized_final_deliverable)

    # Run classifier on unseen data to get positive labels
    posResult = posModel.transform(sanitized_final_deliverable)
    # Rename the 3 new columns to prevent name conflicts
    posResult = posResult.withColumnRenamed("probability", "probability_pos")\
                         .withColumnRenamed("rawPrediction", "rawPrediction_pos")\
                         .withColumnRenamed("prediction", "prediction_pos")
    # Run the classifier on previous positive result to get negative labels too
    result = negModel.transform(posResult)
    # Rename the 3 new columns to make it easier to see which is which
    result = result.withColumnRenamed("probability", "probability_neg")\
                    .withColumnRenamed("rawPrediction", "rawPrediction_neg")\
                    .withColumnRenamed("prediction", "prediction_neg")

    # UDF functions for predicting label based on thresholds
    predict_pos_udf = udf(predict_pos, IntegerType())
    predict_neg_udf = udf(predict_neg, IntegerType())

    # Make predictions based on probability and threshold:
    result = result.select('created_utc', 'author_flair_text', 'link_id', 'id', 'c_score', \
                                 predict_pos_udf(result.probability_pos).alias('pos'),\
                                 predict_neg_udf(result.probability_neg).alias('neg'))
    
    result.write.parquet("result.parquet")
    # result.show()
    
    #---------------------------------------------------------------------------
    # TASK 10
    # Code for task 10... : Perform Analysis on the Predictions
    result = context.read.parquet("result.parquet")
    # Need to JOIN First to Get TITLE of Post

    # print("ABOUT TO EXPLAIN --------------------------------------------------")
    submissions = submissions.select('id', 'title', submissions.score.alias('s_score'))
    result = result.join(submissions, result.link_id == submissions.id)# .explain()
    # result = result.drop('id')
    result.show()
    # print("DONE EXPLAINING ---------------------------------------------------")

    context.registerDataFrameAsTable(result, "result")
    # 1. Percentage of Comments that Were Positive/Negative Across ALL Submissions
    task_10_1 = context.sql("SELECT title, AVG(pos) AS pos_percentage, AVG(neg) AS neg_percentage FROM result GROUP BY title")
    task_10_1.show()

    task_10_1.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("task_10_1.csv")

    # 2. Percentage of Comments that Were Positive/Negative Across ALL Days
    task_10_2 = context.sql("SELECT FROM_UNIXTIME(created_utc, 'Y-M-d') AS day, AVG(pos) AS pos_percentage, AVG(neg) AS neg_percentage FROM result GROUP BY day ORDER BY day asc")
    task_10_2.show()

    task_10_2.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("task_10_2.csv")

    # 3. Percentage of Comments that Were Positive/Negative Across ALL States
    context.registerFunction("check_state_udf", check_state, BooleanType())
    task_10_3 = context.sql("SELECT author_flair_text AS state, AVG(pos) AS pos_percentage, AVG(neg) AS neg_percentage FROM result WHERE check_state_udf(author_flair_text) = True GROUP BY state")
    task_10_3.show()

    task_10_3.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("task_10_3.csv")
    
    # 4A. Percentage of Comments that Were Positive/Negative Across ALL Comments
    task_10_4A = context.sql("SELECT c_score AS comment_score, AVG(pos) AS pos_percentage, AVG(neg) AS neg_percentage FROM result GROUP BY comment_score")
    task_10_4A.show()

    task_10_4A.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("task_10_4A.csv")

    # 4B. Percentage of Comments that Were Positive/Negative Across ALL Story Scores
    task_10_4B = context.sql("SELECT s_score AS submission_score, AVG(pos) AS pos_percentage, AVG(neg) AS neg_percentage FROM result GROUP BY submission_score")
    task_10_4B.show()

    task_10_4B.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("task_10_4B.csv")
    #---------------------------------------------------------------------------
    
if __name__ == "__main__":
    conf = SparkConf().setAppName("CS143 Project 2B")
    conf = conf.setMaster("local[*]")
    sc   = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    sc.addPyFile("cleantext.py")
    # SparkContext.setSystemProperty('spark.driver.memory', '11g') # Sets RAM Memory at 11 GB
    print(sc._conf.getAll())
    main(sqlContext)