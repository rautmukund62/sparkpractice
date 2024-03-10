from pyspark.sql import SparkSession


spark=SparkSession.builder.appName("HIve read write")\
    .enableHiveSupport().getOrCreate()

#read data from hive

df=spark.sql("select * from prod.emp")

df.show()

#process
df1=df.filter(df['city']=='Pune')

df1.show()


#write
df1.write.saveAsTable("prod.emp_pune")