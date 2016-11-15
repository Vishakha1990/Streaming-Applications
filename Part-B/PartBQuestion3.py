
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

import sys
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import *
from pyspark.sql.functions import window



if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: CS-838-Assignment2-PartB-2.py <dataset path> <userid file path>", )
        exit(-1)

    # Initialize the spark context.
    spark = SparkSession\
        .builder\
        .appName("CS-838-Assignment2-PartB-3")\
        .config("spark.master", "spark://10.254.0.169:7077")\
        .config("spark.locality.wait", "0")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[2])
    userSchema = StructType().add("user1", "string").add("user2", "string").add("timestamp", "string").add("tweet", "string")
    words = spark \
        .readStream \
        .option("sep", ",") \
        .schema(userSchema) \
        .csv(sys.argv[1]) 
        #.csv("hdfs:/user/ubuntu/stream-dataset/") 
    
    Counts2 = words.join(lines, words.user1 == lines.value).groupBy('user1').count()


    query = Counts2\
       .writeStream\
       .outputMode("complete")\
       .trigger(processingTime='5 seconds')\
       .option("numRows", "100000000")\
       .option("truncate", "false")\
       .format("console")\
       .start()

    
    query.awaitTermination()
