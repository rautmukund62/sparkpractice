from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from functions import *

spark=SparkSession.builder.appName("Broadcast Join").getOrCreate()

cn='India'

df_emp=read_csv(spark,'D:/emp.csv')
df_dept=read_csv(spark,'D:/dept.csv',delimiter='|')
df1=read_json(spark,'D:/sample.json')
df2=read_json(spark,'D:/sample_mul.json',multiline='true')


df_emp.show()
df_dept.show()
df1.show()
df2.show()