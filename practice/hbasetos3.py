from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("HbasetoS3").getOrCreate()

#Read data from Hbase
df=spark.read.format("org.apache.phoenix.spark").\
    option("zkUrl","localhost:2181").\
    option("table","emp").load()

#process
df1=df.filter(df['city']=="Pune")

#write into s3
df1.write.format("CSV").option("header","true")\
    .save("s3://b12classsayusofttech/output/emp_op")