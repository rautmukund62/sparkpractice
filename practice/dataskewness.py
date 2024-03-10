from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from functions import *
from random import random

spark=SparkSession.builder.appName("Broadcast Join").getOrCreate()


df=read_csv(spark,'D:/emp.csv')

sf=10

df1=df.withColumn("salt_factor",lit(sf*random()))

df2=df1.withColumn("salt_factor",df1['salt_factor'].cast('int'))

df3=df2.withColumn("did_with_sal",concat(df2['salt_factor'],lit('_'),df2['did']))

df4=df3.groupBy(df3['did_with_sal']).count()

df5=df4.withColumn('did', split(df4['did_with_sal'], '_').getItem(1)).drop("did_with_sal")

df5.show()