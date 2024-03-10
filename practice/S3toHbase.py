from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("sample").getOrCreate()

#read  from s3

df=spark.read.format("CSV").option("header","true").option("inferschema","true").load("s3://b12class/input_data/loan-test.csv")

df.show()

df1=df.filter(df["Education"]=='Graduate')

df1.write.format("org.apache.phoenix.spark").option("zkUrl" ,"localhost:2181").option("table","loan").mode('overwrite').save()

print("Data successfully written in Hbase")