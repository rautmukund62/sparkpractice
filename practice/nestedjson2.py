from pyspark.sql import SparkSession

from pyspark.sql.functions import *

spark=SparkSession.builder.appName("complex Json Processing").getOrCreate()

df=spark.read.format("JSON").option("multiline","true").load("D:/nes.json")

df1=df.withColumn("source",explode(array(col("source.*"))))\
    .withColumn("id",col("source.id"))\
    .withColumn("ip",col("source.ip"))\
    .withColumn("description",col("source.description"))\
    .withColumn("temp",col("source.temp"))\
    .withColumn("c02_level",col("source.c02_level"))\
    .withColumn("lat",col("source.geo.lat"))\
    .withColumn("long",col("source.geo.long"))\


df1.show()

df1.printSchema()
