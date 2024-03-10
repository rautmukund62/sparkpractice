from pyspark.sql import  SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("HDFS to Hive")\
    .enableHiveSupport().getOrCreate()

df=spark.read.format("CSV").option("header","true").\
    option("inferschema","true").load("/cust.csv")

df.show()

df.write.saveAsTable("prod.cust")