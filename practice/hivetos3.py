from pyspark.sql import  SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("RDBMS data process")\
    .enableHiveSupport().getOrCreate()

df=spark.sql("select * from prod.cust_hemant where city='pune'")

df.show()

df.write.format("json").save("s3://classusa/output/cust_hemant")
