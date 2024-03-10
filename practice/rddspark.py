from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("sample").getOrCreate()

ls=[10,20,30,40,50]

rdd=spark.sparkContext.parallelize(ls)

rdd1=rdd.map(lambda x:x*10)

print(rdd1.collect())

rdd1.saveAsTextFile("D:/op")