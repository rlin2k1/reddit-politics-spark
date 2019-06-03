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
from pyspark.sql import functions
from pyspark.sql.functions import udf # Spark User Defined Function
# UDF Return Types
from pyspark.sql.types import ArrayType, StringType, IntegerType, BooleanType
from cleantext import sanitize # Tokenizer
import pandas as pd # Pandas DataFrame Printing

from pyspark.ml.classification import LogisticRegression
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder, CrossValidatorModel 
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.feature import CountVectorizerModel



def get_index_1(vec):
    return str(vec[1])

def get_index_0(vec):
    return str(vec[0])


def predict_pos(vec):
    if float(vec[1]) > 0.2:
        return 1
    else:
        return 0

def predict_neg(vec):
    if float(vec[1]) > 0.25:
        return 1
    else:
        return 0



import re # Regex
# from pyspark.sql import functions as F # Assign Boolean Values to Functions

def strip_t3(text):
    """Strips first 3 characters of text."""
    return text[3:]

def sarcastic_or_quote(text):
    """Returns true if string contains '/s' or starts with &gt"""
    sarcastic_or_quote_match = re.compile(r'^&gt|\/s')
    return not sarcastic_or_quote_match.search(text)

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
    # TASK 2
    labeled_comments = labeled_data.join(comments, comments.id == labeled_data.Input_id)
    labeled_comments = labeled_comments.select('Input_id', 'labeldjt', 'body')
    # labeled_comments.show()

    #---------------------------------------------------------------------------
    # TASK 4
    sanitize_udf = udf(sanitize, ArrayType(StringType()))

    """ # TODO uncomment
    #---------------------------------------------------------------------------
    # TASK 5
    sanitized_labeled_comments = labeled_comments.select('Input_id', 'labeldjt', sanitize_udf('body').alias('raw'))
    # df = sanitized_labeled_comments.limit(10).toPandas() # Pretty Printing Only
    # print(df) # Pretty Printing Only

    #---------------------------------------------------------------------------
    # TASK 6A
    cv = CountVectorizer(binary=True, minDF=10.0, inputCol="raw", outputCol="features")
    model = cv.fit(sanitized_labeled_comments)
    sanitized_labeled_comments = model.transform(sanitized_labeled_comments)
    sanitized_labeled_comments.show(truncate=False)
    countVectorizerPath = "count_vectorizer_model"
    # model.save(countVectorizerPath)

    #---------------------------------------------------------------------------
    # TASK 6B
    # Labels: {1, 0, -1, -99}
    pos = sanitized_labeled_comments.select(sanitized_labeled_comments.features, sanitized_labeled_comments.labeldjt.cast(IntegerType()))
    pos = pos.withColumnRenamed("labeldjt", "label")
    pos = pos.replace(-1, 0)
    pos = pos.replace(-99, 0)
    pos.show()

    neg = sanitized_labeled_comments.select(sanitized_labeled_comments.features, sanitized_labeled_comments.labeldjt.cast(IntegerType()))
    neg = neg.withColumnRenamed("labeldjt", "label")
    neg = neg.replace(-1, 1)
    neg = neg.replace(1, 0)
    neg = neg.replace(-99, 0)
    neg.show()

    #---------------------------------------------------------------------------
    # TASK 7: MACHINE LEARNING PORTION TO TRAIN MODELS
    # Initialize two logistic regression models.
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
    """ # TODO uncomment
    #---------------------------------------------------------------------------
    # TASK 8: Make Final Deliverable for Unseen Data - We don't need labeled_data anymore
    strip_t3_udf = udf(strip_t3, StringType())
    sarcastic_or_quote_udf = udf(sarcastic_or_quote, BooleanType())
    # Get Unseen Data
    sanitized_final_deliverable = comments.select('created_utc', strip_t3_udf(comments.link_id).alias('link_id'), 'author_flair_text', 'id', 'body', sanitize_udf('body').alias('raw'))\
        .filter(sarcastic_or_quote_udf(comments['body'])) #F.when(comments["body"].rlike('^&gt|\/s'), False).otherwise(True))
    sanitized_final_deliverable.show()

    #---------------------------------------------------------------------------
    # TASK 9

    # TODO DELETE
    model = CountVectorizerModel.load("count_vectorizer_model") # TODO DELETE BEFORE SUBMITTING
    posModel = CrossValidatorModel.load("project2/pos.model") # TODO DELETE BEFORE SUBMITTING
    negModel = CrossValidatorModel.load("project2/neg.model") # TODO DELETE BEFORE SUBMITTING

    # Run the CountVectorizerModel on Task 8 relation
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
    result = result.select('created_utc', 'author_flair_text', 'link_id', 'id', 'raw', 'probability_pos', 'probability_neg',\
                                 predict_pos_udf(result.probability_pos).alias('pos'),\
                                 predict_neg_udf(result.probability_neg).alias('neg'))

    result.show()
    

if __name__ == "__main__":
    conf = SparkConf().setAppName("CS143 Project 2B")
    conf = conf.setMaster("local[*]")
    sc   = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    sc.addPyFile("cleantext.py")
    main(sqlContext)