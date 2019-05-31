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

    comments = context.read.parquet("comments.parquet") #context is spark
    submissions = context.read.parquet("submissions.parquet")
    labels = context.read.parquet("labels.parquet")
    #comments.show()
    #submissions.show()
    #labels.show()

    # TASK 2
    # Code for task 2
    labeled_comments = labels.join(comments, comments.id == labels.Input_id).select(['Input_id', 'labeldem', 'labelgop', 'labeldjt', 'body'])
    #labeled_comments.show()

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
    
    transform_data = udf(transform_data)
    labeled_comments.select('Input_id','labeldem','labelgop','labeldjt','body', transform_data('body').alias("grams")).show()
    
    from pyspark.sql.types import ArrayType, StringType, BooleanType, DoubleType, IntegerType
    #TASK 6
    from pyspark.ml.feature import CountVectorizer

# Input data: Each row is a bag of words with a ID.
    '''
    from pyspark.sql import SparkSession
    from os.path import expanduser, join, abspath
    warehouse_location = abspath('spark-warehouse')
    spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Hive integration example") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()
    df1 = spark.createDataFrame([
    (0, "a b c".split(" ")),
    (1, "a b b c a".split(" "))
    ], ["id", "words"])
    cv1 = CountVectorizer(inputCol="words", outputCol="features", vocabSize=3, minDF=1.0) #vocab size is count of unique different strings ig

    df1.show()
    print("i aint a nonetype")
    model1 = cv1.fit(df1)
    result1 = model1.transform(df1)
    result1.show(truncate=False)
    EXAMPLE IF INTERESTED IN HOW IT WORKS ABOVE
    '''
    
    #Task 6A
    df=labeled_comments.select('Input_id','labeldem','labelgop','labeldjt','body', transform_data('body').alias("grams"))
    from pyspark.sql.functions import col, split
    df=df.withColumn("grams", split(col("grams"), ",\s*").cast(ArrayType(StringType())).alias("grams"))
    #df.show()

    cv = CountVectorizer(inputCol="grams", outputCol="features", binary=True, minDF=10.0)
    model = cv.fit(df)
    result = model.transform(df)
    result.show()

    #Task 6B

    def fp(row):
        if row== 1:
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


    import pyspark.sql.functions as f
    #result['label']=result.apply(fp, axis=1) #fake news btw, apply doesnt work here
    positive_df=result
    fp = udf(fp)
    fn =udf(fn)
    positive_df=result.select("*", fp('labeldjt').alias("label")) #it may not seem like it, but this code segment here almost killed me
    negative_df=result.select("*", fn('labeldjt').alias("label"))
    positive_df=positive_df.withColumn("label", (col("label")).cast(IntegerType()).alias("label"))#it may not seem like it, but this code segment here almost killed me
    negative_df=negative_df.withColumn("label", (col("label")).cast(IntegerType()).alias("label"))

    positive_df.show()
    negative_df.show()


    #Task 7
    #MAKE SURE TO UNCOMMONT OUT THE triple ' to create the model!
    # Bunch of imports (may need more)
    '''
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
'''
#Task 8
	
	#TODO: join submissions and comments, but need to get state. think the utc thing was timestamp of when comment created, and tilte submission was obtained from join, but the 3rd one we need to strip and do a bit more work. leaving that to u
    #actually doesnt work so rip, i kidna give up for now. current error: 
#AnalysisException: "Reference 'created_utc' is ambiguous, could be: created_utc, created_utc.;"
#i tried to do something like comments.created_utc but that also doesnt work 
    #more_comments = submissions.join(comments, comments.link_id == submissions.id).select(['created_utc', 'title', 'author_flair_text'])#, 'labeldjt', 'body'])
    #comments.show()
    #submissions.show()
    #more_comments.show()







if __name__ == "__main__":
    conf = SparkConf().setAppName("CS143 Project 2B")
    conf = conf.setMaster("local[*]")
    sc   = SparkContext(conf=conf)
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