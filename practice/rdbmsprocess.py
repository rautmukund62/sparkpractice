from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("RDBMS processing").getOrCreate()

#read
df=spark.read.format("JDBC").\
    option("url","jdbc:mysql://database-1.cxau8dx2g7op.ap-south-1.rds.amazonaws.com:3306/PROD").\
    option("user","myuser").\
    option("password","mypassword").\
    option("dbtable","emp").\
    option("driver","com.mysql.cj.jdbc.Driver").\
    load()

df.show()

#process
df1=df.filter(df['sal']>70000)

df1.show()

#write
df1.write.format("CSV").option("header","true").save("/empop")

#df1.write.format("JDBC").\
#    option("url","jdbc:mysql://database-1.cxau8dx2g7op.ap-south-1.rds.amazonaws.com:3306/PROD").\
#    option("user","myuser").\
#    option("password","mypassword").\
#    option("dbtable","emp1").\
#    option("driver","com.mysql.cj.jdbc.Driver").\
#    save()