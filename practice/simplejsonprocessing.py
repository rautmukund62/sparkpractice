from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("Simple Json Processing").getOrCreate()

df=spark.read.format("JSON").load("D:/cars.json")

df1=df.filter(df['price'] >= 50000)

df1.show()