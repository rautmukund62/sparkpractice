from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType #import types for UDF return type

spark=SparkSession.builder.appName("sample").getOrCreate()

df=spark.read.format("CSV").option("header","true").\
    option("inferschema","true").load("D:/temp.csv")

#df.show()

df.printSchema()

#define Function

def convert_to_zero(num):
    if num >= 0:
        return num
    else:
        return 0

#register UDF
convert_to_zero=udf(convert_to_zero,IntegerType())

#apply UDF to column

df1=df.withColumn("temp",convert_to_zero(df['temp']))

df1.show()

df1.printSchema()

