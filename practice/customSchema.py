from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Row

spark=SparkSession.builder.appName("sample").getOrCreate()

rdd=spark.sparkContext.textFile("D:/temp.csv")

schema=Row("year","temp")

def trx(line):
 x=line.split(',')
 return x[0],x[1]


df=rdd.map(trx).map(lambda x:schema(*x)).toDF()

df.show()