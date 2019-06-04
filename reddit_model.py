from __future__ import print_function
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext


def main(context):
    """Main function takes a Spark SQL context."""
    # YOUR CODE HERE
    # YOU MAY ADD OTHER FUNCTIONS AS NEEDED

    read_data = True
    create_models = True

    # TASK 1
    # Code for task 1
    if read_data:
        sqlContext = SQLContext(context)
        comments = sqlContext.read.json("comments-minimal.json.bz2")
        submissions = sqlContext.read.json("submissions.json.bz2")
        labels = sqlContext.read.format('csv').options(header='true',inferSchema='true').load("labeled_data.csv")
        comments.write.parquet("comments.parquet")
        submissions.write.parquet("submissions.parquet")
        labels.write.parquet("labels.parquet")
    else:
        # sudo update-alternatives --config java (choose java version 8)
        comments = context.read.parquet("comments.parquet") # context is spark
        submissions = context.read.parquet("submissions.parquet")
        labels = context.read.parquet("labels.parquet")

    # TASK 2
    # Code for task 2
    if read_data:
        labeled_comments = labels.join(comments, comments.id == labels.Input_id).select(['Input_id', 'labeldem', 'labelgop', 'labeldjt', 'body'])

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

    if create_models:
        labeled_comments = labeled_comments.select('Input_id', 'labeldem', 'labelgop', 'labeldjt', 'body', udf_transform_data('body').alias("grams"))

    # TASK 6A
    from pyspark.sql.types import ArrayType, StringType, IntegerType
    from pyspark.ml.feature import CountVectorizer, CountVectorizerModel
    from pyspark.sql.functions import col, split

    if create_models:
        labeled_comments = labeled_comments.withColumn("grams", split(col("grams"), ",\s*").cast(ArrayType(StringType())).alias("grams"))
        cv = CountVectorizer(inputCol="grams", outputCol="features", binary=True, minDF=10.0)
        model = cv.fit(labeled_comments)
        result = model.transform(labeled_comments)
        model.save('countvectorizer.model')
    else:
        model = CountVectorizerModel.load("countvectorizer.model")

    # TASK 6B
    if create_models:
        positive_df = result.select("*", udf(lambda row : 1 if row == 1 else 0)('labeldjt').alias("label")) #it may not seem like it, but this code segment here almost killed me
        negative_df = result.select("*", udf(lambda row : 1 if row == -1 else 0)('labeldjt').alias("label"))
        positive_df = positive_df.withColumn("label", (col("label")).cast(IntegerType()).alias("label")) #it may not seem like it, but this code segment here almost killed me
        negative_df = negative_df.withColumn("label", (col("label")).cast(IntegerType()).alias("label"))

    # TASK 7
    if create_models:
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
        pos_model = posCrossval.fit(posTrain)
        print("Training negative classifier...")
        neg_model = negCrossval.fit(negTrain)

        # Once we train the models, we don't want to do it again. We can save the models and load them again later.
        pos_model.save("pos.model")
        neg_model.save("neg.model")
    else:
        from pyspark.ml.tuning import CrossValidatorModel
        pos_model = CrossValidatorModel.load("pos.model")
        neg_model = CrossValidatorModel.load("neg.model")

    # TASK 8
    # Code for task 8
    c = comments.sample(False, 0.001, None) 
    #c=comments
    c = c.select('created_utc', 'body', col('score').alias('comment_score'), col('author_flair_text').alias('state'), udf(lambda x : x[3:])('link_id').alias('link_id'))
    c = c.filter(~c.body.contains('/s') & ~c.body.startswith('&gt;'))
    s = submissions.select('id', 'title', col('score').alias('submission_score'))
    cs = s.join(c, c.link_id == s.id)

    # TASK 9
    # Code for task 9
    #cs = cs.filter(~cs.body.contains('/s') & ~cs.body.startswith('&gt;')) # filtering in earlier step for optimization
    cs = cs.select('link_id', 'created_utc', 'state', 'comment_score', 'submission_score', 'title', udf_transform_data('body').alias("grams"))
    cs = cs.withColumn("grams", split(col("grams"), ",\s*").cast(ArrayType(StringType())).alias("grams"))
    cs = model.transform(cs)
    pos_result = pos_model.transform(cs)
    pos_result = pos_result.select('comment_score', 'submission_score','created_utc', 'title', 'state', 'link_id', udf(lambda x : 1 if x[1] > 0.2 else 0)('probability').alias('pos').cast(IntegerType()),col('rawPrediction').alias('pos_rawPrediction'),col('prediction').alias('pos_pred'), 'features')
    t_result = neg_model.transform(pos_result)

    t_result = t_result.select('comment_score', 'submission_score','created_utc', 'title', 'state', 'link_id', udf(lambda x : 1 if x[1] > 0.25 else 0)('probability').alias('neg').cast(IntegerType()),'pos',col('rawPrediction').alias('neg_rawPrediction'),col('prediction').alias('neg_pred'), 'pos_rawPrediction', 'pos_pred')

    # TASK 10
    # Code for task 10
    import pyspark.sql.functions as func
    # 10.1
    id_values = t_result.groupBy(col('link_id').alias('l_id')).agg(func.sum('pos').alias('count_pos'),func.sum('neg').alias('count_neg')) #need to get it to also have title column not just link_id
    id_count = t_result.groupBy('link_id', 'title').agg(func.count('*').alias('total'))

    id_values_ratio = id_values.join(id_count, id_count.link_id == id_values.l_id).withColumn('Positive', col('count_pos') / col('total')).withColumn('Negative',col('count_neg')/col('total'))
    id_values_ratio = id_values_ratio.select('link_id','title','Positive','Negative')

    # 10.2
    utc_values = t_result.groupBy(func.from_unixtime('created_utc', 'yyyy-MM-dd').alias('d')).agg(func.sum('pos').alias('count_pos'),func.sum('neg').alias('count_neg'))
    utc_count = t_result.groupBy(func.from_unixtime('created_utc', 'yyyy-MM-dd').alias('date')).agg(func.count('*').alias('total'))

    utc_values_ratio = utc_values.join(utc_count, utc_count.date == utc_values.d).withColumn('Positive', col('count_pos') / col('total')).withColumn('Negative',col('count_neg')/col('total'))
    utc_values_ratio=utc_values_ratio.select('date','Positive','Negative')

    # 10.3
    states = ['Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware', 'District of Columbia', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire', 'New Jersey', 'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming']
    state_values = t_result.filter(col('state').isin(states)).groupBy(col('state').alias('s')).agg(func.sum('pos').alias('count_pos'),func.sum('neg').alias('count_neg'))
    state_count = t_result.filter(col('state').isin(states)).groupBy('state').agg(func.count('*').alias('total'))

    state_values_ratio = state_values.join(state_count, state_count.state == state_values.s).withColumn('Positive', col('count_pos') / col('total')).withColumn('Negative',col('count_neg')/col('total'))
    state_values_ratio = state_values_ratio.select('state','Positive','Negative')

    # 10.4.comments_score
    comment_score_values = t_result.groupBy(col('comment_score').alias('c_score')).agg(func.sum('pos').alias('count_pos'),func.sum('neg').alias('count_neg'))
    comment_score_count = t_result.groupby('comment_score').agg(func.count('*').alias('total'))

    comment_score_values_ratio = comment_score_values.join(comment_score_count, comment_score_count.comment_score == comment_score_values.c_score).withColumn('Positive', col('count_pos') / col('total')).withColumn('Negative',col('count_neg')/col('total'))
    comment_score_values_ratio=comment_score_values_ratio.select('comment_score','Positive','Negative')

    # 10.4.submissions_score
    submission_score_values = t_result.groupBy(col('submission_score').alias('s_score')).agg(func.sum('pos').alias('count_pos'),func.sum('neg').alias('count_neg'))
    submission_score_count = t_result.groupby('submission_score').agg(func.count('*').alias('total'))

    submission_score_values_ratio = submission_score_values.join(submission_score_count, submission_score_count.submission_score == submission_score_values.s_score).withColumn('Positive', col('count_pos') / col('total')).withColumn('Negative',col('count_neg')/col('total'))
    submission_score_values_ratio=submission_score_values_ratio.select('submission_score','Positive','Negative')

    # PLOTS
    id_values_ratio.orderBy("Positive", ascending=False).limit(10).repartition(1).write.format("com.databricks.spark.csv").option("header","true").save("Top_Positive_Submissions.csv")
    id_values_ratio.orderBy("Negative", ascending=False).limit(10).repartition(1).write.format("com.databricks.spark.csv").option("header","true").save("Top_Negative_Submissions.csv") #idk if this is good
    utc_values_ratio.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("time_data.csv")
    state_values_ratio.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("state_data.csv")

    # Last 2 csv
    submission_score_values_ratio.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("submission_score.csv") #NOTE this should be doing something with like limit 10 based on spec. so dataframe should be limiting this, or i can limit it in between somewhere.
    comment_score_values_ratio.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("comment_score.csv")


if __name__ == "__main__":
    conf = SparkConf().setAppName("CS143 Project 2B")
    conf = conf.setMaster("local[*]")
    sc   = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    sc.addPyFile("cleantext.py")
    main(sqlContext)
