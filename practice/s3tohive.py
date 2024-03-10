from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("S3toHive").enableHiveSupport().getOrCreate()


#read data from s3

df=spark.read.format("JSON").load("s3://b12class/JSON_Data/cars.json")

df.show()

#write data in hive
df.write.saveAsTable("prod.cars_details")