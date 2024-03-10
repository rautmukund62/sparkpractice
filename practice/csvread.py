
from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("sample").getOrCreate()

#read
df=spark.read.format("json").load("D:/cars.json")

df.show()

