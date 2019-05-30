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

    readcomments = context.read.parquet("comments.parquet")
    readsubmissions = context.read.parquet("submissions.parquet")
    readlabels = context.read.parquet("labels.parquet")

    #wow just fuucking use this one...zzzzzzzzzzzz
    readcomments.show()
    readsubmissions.show()
    readlabels.show()

    """
    # what does this stuff do? - qt
    from pyspark.sql import SparkSession
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    #df = spark.read.json("submissions.json.bz2")
    # Displays the content of the DataFrame to stdout
    #df.show()
    """

    # Task 2


    # Task 3


    # Task 4
    

    def foo(s):
        return str(s) + "aaaaaaaaa"
    from pyspark.sql.functions import udf
    foo = udf(foo)
    readlabels.select('Input_id', foo('Input_id').alias("aaa")).show()


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