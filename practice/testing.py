from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("testing").getOrCreate()

ls=[10,20,30]

rdd=spark.sparkContext.parallelize(ls)

print(rdd.collect())