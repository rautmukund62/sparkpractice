from pyspark.sql import SparkSession

from pyspark.sql.functions import *

spark=SparkSession.builder.appName("complex Json Processing").getOrCreate()

df=spark.read.format("JSON").option("multiline","true").load("D:/persons.json")

df1=df.withColumn("persons",explode(df['persons']))\
.withColumn("cars",explode(col("persons.cars")))\
.withColumn("models",explode(col("cars.models")))\
    .select("persons.name","persons.age","cars.name","models")

df1.show()