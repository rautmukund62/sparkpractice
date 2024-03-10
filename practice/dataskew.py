from pyspark.sql import SparkSession
import random
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("data skewness").getOrCreate()

df=spark.read.format("CSV").option("header","true").option("inferschema","true").load("D:/emp.csv")



df1=df.withColumn("salt_factor",lit(int(random.random()*10)))

df2=df1.withColumn("did_with_sf",concat(df1["did"],lit("_"),df1["salt_factor"]))

df3=df2.select("eid","ename","did_with_sf")

#process

df4=df3.withColumn("did",split(df3["did_with_sf"],"_")[0])

df4.show()