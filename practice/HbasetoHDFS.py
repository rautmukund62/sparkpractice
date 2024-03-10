from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("Without header").getOrCreate()

df=spark.read.format("CSV").option("header","true").option("inferSchema","true").load("D:/data.csv")
df.show()
df.printSchema()

#inbuilt
#df1=df.withColumn("country",lower(df["country"]))
#df1.show()

#UDF
def make_temp_zero(num):
    if num <= 0:
        return 0
    else:
        return num

#register function
make_temp_zero=udf(make_temp_zero,IntegerType())
#use
df2=df.withColumn("temp",make_temp_zero(df['temp']))
df2.show()