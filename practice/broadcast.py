from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from allfunctions import *

spark=SparkSession.builder.appName("Broadcast join").getOrCreate()

df_emp=read_csv(spark,'D:/emp.csv')

df_emp.show()
df_dept=read_csv(spark,'D:/dept.csv',delimiter='|')
df_dept.show()

df_res=df_emp.join(broadcast(df_dept),"did","inner")

df_res.show()

df=read_json(spark,'D:/sample.json')

df.show()