from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("Simple Json Processing").getOrCreate()

spark.sparkContext.setLogLevel("OFF")

df=spark.read.format("JSON").load("D:/sample.json")

df.show()
