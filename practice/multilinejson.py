from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("Simple Json Processing").getOrCreate()

df=spark.read.format("JSON").option("multiline","true").load("D:/sample_mul.json")

df.show()
