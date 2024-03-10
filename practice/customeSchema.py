from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark=SparkSession.builder.appName("HbasetoS3").getOrCreate()


#define Schema

schema=StructType([
    StructField("roll",IntegerType(),True),
    StructField("name",StringType(),True),
    StructField("marks",IntegerType(),True),
    StructField("mob",IntegerType(),True)
])

#read

df=spark.read.format("CSV").option("header","false").\
    option("inferschema","true").schema(schema).load("D:/std.csv")

df.show()

