from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("HbasetoS3").getOrCreate()

#Read data from S3

df=spark.read.format("CSV").option("header","true").\
    option("inferschema","true").\
    load("s3://b12classsayusofttech/input/std.csv")

#processing

df1=df.filter(df['marks'] >= 45)

#write Hbase
df1.write.format("org.apache.phoenix.spark").\
    option("zkUrl","localhost:2181").\
    option("table","STD").mode('overwrite').save()
