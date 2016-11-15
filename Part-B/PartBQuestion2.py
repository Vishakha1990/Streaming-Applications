
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import *
from pyspark.sql.functions import window
import sys


if __name__ == "__main__":
    #if len(sys.argv) != 3:
    #    print("Usage: CS-838-Assignment2-PartB-2.py <dataset path> <parquet-file-path>")
    #    exit(-1)

    #dataset path = hdfs:/user/ubuntu/stream-dataset/
    #parquet = /user/ubuntu/PartB-outlog

    # Initialize the spark context.
    spark = SparkSession\
        .builder\
        .appName("CS-838-Assignment2-PartB-2")\
        .config("spark.master", "spark://10.254.0.169:7077")\
        .config("spark.locality.wait", "0")\
        .getOrCreate()



    userSchema = StructType().add("user1", "string").add("user2", "string").add("timestamp", "string").add("tweet", "string")
    words = spark \
        .readStream \
        .option("sep", ",") \
        .schema(userSchema) \
        .csv(sys.argv[1]) 
        
    Counts = words.select("user2").where("tweet == 'MT'")
    
    
    query = Counts\
       .writeStream\
       .outputMode("append")\
       .format("parquet")\
       .trigger(processingTime='10 seconds')\
       .option("checkpointLocation", "/user/ubuntu/PartB-outlog-new")\
       .start(path=sys.argv[2])
    
    
    query.awaitTermination()
