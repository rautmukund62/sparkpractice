from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("Sample").getOrCreate()

df=spark.read.format("JSON").load("file:///D:/cars.json")

df1=df.filter(df['kind']=='electric')

df2=df1.agg(sum(df1.price))

df2.show()

