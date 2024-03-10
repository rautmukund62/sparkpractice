from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("spark UDF").getOrCreate()


df=spark.read.format("CSV").option("header","true")\
    .option("inferschema","true").load("D:/temp.csv")

#define

def negative_to_zero(num):
    if num <= 0:
        return 0
    else:
        return num

#register function/UDF

negative_to_zero=udf(negative_to_zero,IntegerType())

#apply

df1=df.withColumn("temp",negative_to_zero(df["temp"]))


df1.show()