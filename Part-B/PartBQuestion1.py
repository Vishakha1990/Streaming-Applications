
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import *
from pyspark.sql.functions import window
import sys



if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: CS-838-Assignment2-PartB-1.py <dataset path>")
        exit(-1)

    # Initialize the spark context.
    spark = SparkSession\
        .builder\
        .appName("CS-838-Assignment2-PartB-1")\
        .config("spark.master", "spark://10.254.0.169:7077")\
        .config("spark.locality.wait", "0")\
        .getOrCreate()



userSchema = StructType().add("user1", "string").add("user2", "string").add("timestamp", "string").add("tweet", "string")
words = spark \
    .readStream \
    .option("sep", ",") \
    .schema(userSchema) \
    .csv(sys.argv[1]) 
    



windowedCounts = words.groupBy(
    window(words.timestamp, '60 minutes', '30 minutes'),
    words.tweet
).count()


query = windowedCounts\
   .writeStream\
   .outputMode("complete")\
   .option("numRows", "100000000")\
   .option("truncate", "false")\
   .format("console")\
   .start()

query.awaitTermination()
