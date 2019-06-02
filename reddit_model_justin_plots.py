from __future__ import print_function
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext


def main(context):
    """Main function takes a Spark SQL context."""
    # YOUR CODE HERE
    # YOU MAY ADD OTHER FUNCTIONS AS NEEDED

    """
    # TASK 1
    # Code for task 1
    sqlContext = SQLContext(context)
    comments = sqlContext.read.json("comments-minimal.json.bz2")
    submissions = sqlContext.read.json("submissions.json.bz2")
    labels = sqlContext.read.format('csv').options(header='true',inferSchema='true').load("labeled_data.csv")
    comments.write.parquet("comments.parquet")
    submissions.write.parquet("submissions.parquet")
    labels.write.parquet("labels.parquet")
    """

    # sudo update-alternatives --config java (choose java version 8)

    comments = context.read.parquet("comments.parquet") # context is spark
    submissions = context.read.parquet("submissions.parquet")
    labels = context.read.parquet("labels.parquet")

    #comments=comments.sample(False,0.2,None)
    #submissions=submissions.sample(False,0.2,None)
    #labels=labels.sample(False,0.2,None)

    # TASK 2
    # Code for task 2
    labeled_comments = labels.join(comments, comments.id == labels.Input_id).select(['Input_id', 'labeldem', 'labelgop', 'labeldjt', 'body'])

    #labeled_comments=labeled_comments.sample(False,0.2,None)
    # TASK 3
    # Code for task 3
    # Removed from spec

    # TASKS 4, 5
    # Code for tasks 4 and 5
    import cleantext
    from pyspark.sql.functions import udf
    def transform_data(text):
        res = []
        for gram in cleantext.sanitize(text):
            res += gram.split()
        return res
    
    udf_transform_data = udf(transform_data)
    labeled_comments = labeled_comments.select('Input_id', 'labeldem', 'labelgop', 'labeldjt', 'body', udf_transform_data('body').alias("grams"))
    
    # TASK 6A
    from pyspark.sql.types import ArrayType, StringType, IntegerType
    from pyspark.ml.feature import CountVectorizer
    from pyspark.sql.functions import col, split

    labeled_comments = labeled_comments.withColumn("grams", split(col("grams"), ",\s*").cast(ArrayType(StringType())).alias("grams"))

    cv = CountVectorizer(inputCol="grams", outputCol="features", binary=True, minDF=10.0)
    model = cv.fit(labeled_comments)
    result = model.transform(labeled_comments)

    # TASK 6B
    def fp(row):
        if row == 1:
            val = 1
        else:
           val = 0
        return val

    def fn(row):
        if row == -1:
           val = 1
        else:
           val = 0
        return val

    fp = udf(fp)
    fn = udf(fn)
    positive_df = result.select("*", fp('labeldjt').alias("label")) #it may not seem like it, but this code segment here almost killed me
    negative_df = result.select("*", fn('labeldjt').alias("label"))
    positive_df = positive_df.withColumn("label", (col("label")).cast(IntegerType()).alias("label")) #it may not seem like it, but this code segment here almost killed me
    negative_df = negative_df.withColumn("label", (col("label")).cast(IntegerType()).alias("label"))

    # TASK 7
    #MAKE SURE TO UNCOMMONT OUT THE triple ' to create the model!
    """
    # Bunch of imports (may need more)
    from pyspark.ml.classification import LogisticRegression
    from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
    from pyspark.ml.evaluation import BinaryClassificationEvaluator
    # Initialize two logistic regression models.
    # Replace labelCol with the column containing the label, and featuresCol with the column containing the features.
    poslr = LogisticRegression(labelCol="label", featuresCol="features", maxIter=10).setThreshold(0.25)
    neglr = LogisticRegression(labelCol="label", featuresCol="features", maxIter=10).setThreshold(0.25)
    # This is a binary classifier so we need an evaluator that knows how to deal with binary classifiers.
    posEvaluator = BinaryClassificationEvaluator()
    negEvaluator = BinaryClassificationEvaluator()
    # There are a few parameters associated with logistic regression. We do not know what they are a priori.
    # We do a grid search to find the best parameters. We can replace [1.0] with a list of values to try.
    # We will assume the parameter is 1.0. Grid search takes forever.
    posParamGrid = ParamGridBuilder().addGrid(poslr.regParam, [1.0]).build()
    negParamGrid = ParamGridBuilder().addGrid(neglr.regParam, [1.0]).build()
    # We initialize a 5 fold cross-validation pipeline.
    posCrossval = CrossValidator(
        estimator=poslr,
        evaluator=posEvaluator,
        estimatorParamMaps=posParamGrid,
        numFolds=5)
    negCrossval = CrossValidator(
        estimator=neglr,
        evaluator=negEvaluator,
        estimatorParamMaps=negParamGrid,
        numFolds=5)
    # Although crossvalidation creates its own train/test sets for
    # tuning, we still need a labeled test set, because it is not
    # accessible from the crossvalidator (argh!)
    # Split the data 50/50
    posTrain, posTest = positive_df.randomSplit([0.5, 0.5])
    negTrain, negTest = negative_df.randomSplit([0.5, 0.5])
    # Train the models
    print("Training positive classifier...")
    posModel = posCrossval.fit(posTrain)
    print("Training negative classifier...")
    negModel = negCrossval.fit(negTrain)
    # Once we train the models, we don't want to do it again. We can save the models and load them again later.
    posModel.save("project2/pos.model")
    negModel.save("project2/neg.model")
    """

    # TASK 8
	# Code for task 8
    from pyspark.ml.tuning import CrossValidatorModel

    c = comments.select('created_utc', 'body', col('author_flair_text').alias('state'), udf(lambda x : x[3:])('link_id').alias('link_id'))
    s = submissions.select('id', 'title')
    cs = s.join(c, c.link_id == s.id)

    # TASK 9
    # Code for task 9
    cs = cs.filter(~cs.body.contains('/s') & ~cs.body.startswith('&gt;')) #remove sarcasm gt	


    cs = cs.select('created_utc', 'title', 'state', 'id', udf_transform_data('body').alias("grams"))
    cs = cs.withColumn("grams", split(col("grams"), ",\s*").cast(ArrayType(StringType())).alias("grams"))
    cs = model.transform(cs) #yes
    pos_model = CrossValidatorModel.load("project2/pos.model")
    neg_model = CrossValidatorModel.load("project2/neg.model")
    pos_result = pos_model.transform(cs)
    neg_result = neg_model.transform(cs)

    pos_result = pos_result.select('*', udf(lambda x : 1 if x[1] > 0.2 else 0)('probability').alias('pos').cast(IntegerType())) #df [ps]
    neg_result = neg_result.select('*', udf(lambda x : 1 if x[1] > 0.25 else 0)('probability').alias('neg').cast(IntegerType())) #df neg
    pos_result.show()
    neg_result.show()

    #testing plot stuff
    pos_result.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("positivedataframe.csv")
    neg_result.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("negativedataframe.csv")

    # TASK 10
    # Code for task 10
    # states = ['Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware', 'District of Columbia', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire', 'New Jersey', 'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming']
    #should we filter more here...

    pos_sum = pos_result.groupBy('id').sum('pos').collect()[0][0]
    pos_count = pos_result.count()
    print(pos_sum) #positive comments all submissions
    print(pos_count)
    print(pos_sum / pos_count)

    neg_sum = neg_result.groupBy('id').sum('neg').collect()[0][0]
    neg_count = neg_result.count()
    print(neg_sum) #ngatvie comments all submissions
    print(neg_count)
    print(neg_sum / neg_count)

    #10.2
    pos_sum_utc = pos_result.groupBy(CAST(FROM_UNIXTIME(created_utc, 'yyyy-MM-dd'))).sum('pos').collect()[0][0] #this may be wrong, i havent gotten to testing it.
    pos_count_utc = pos_result.count()
    print(pos_sum) #positive comments
    print(pos_count)
    print(pos_sum / pos_count)

    neg_sum = neg_result.groupBy(CAST(FROM_UNIXTIME(created_utc, 'yyyy-MM-dd'))).sum('neg').collect()[0][0]
    neg_count = neg_result.count()
    print(neg_sum) #ngatvie comments
    print(neg_count)
    print(neg_sum / neg_count)

    #10.3
	#neg_result.filter(col('state').isin(states)).groupBy('state').agg(count("*")).show()



	#PLOTTING STUFF


    #3 for plotting

    #for learning purposes
    #pos_sum_utc.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("psubmissions.csv")
    #neg_sum.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("nsubmissions.csv")

    #goal is to get it to this point so i can use 4 dataframes below and partition them, then save into and use the 4 csv files below
    total_utc.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("time_data.csv")
    total_state.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("state_data.csv")
    total_submission_score.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("submission_score.csv") #NOTE this should be doing something with like limit 10 based on spec. so dataframe should be limiting this, or i can limit it in between somewhere.
    total_comment_score.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("comment_score.csv")



if __name__ == "__main__":
    conf = SparkConf().setAppName("CS143 Project 2B")
    conf = conf.setMaster("local[*]")
    sc   = SparkContext(conf=conf)
    sc.setLogLevel("WARN") # remove b4 submit
    sqlContext = SQLContext(sc)
    sc.addPyFile("cleantext.py")
    main(sqlContext)

"""
# CONVERT TO PDF
Question 1:
Input_id -> labeldem
Input_id -> labelgop
Input_id -> labeldjt
Question 2:
No the data is not fully normalized. We could decompose it further by moving all attributes relating to the author into another relation and using an author_id. Also, we could decompose it further by moving all attributes relating to the subreddit into another relation and only using the subreddit_id. 
I think the data collector stored the data like this to make it easier to perform statistics on the data. It would be easier because we don't need to perform joins. Also, it may be that the data was made available in this format and so it was just collected as is. 
"""