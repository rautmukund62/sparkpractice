from pyspark.sql import  SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("RDBMS data process").getOrCreate()

#read data from RDBMS - mysql

df_emp=spark.read.format("JDBC").option("url","jdbc:mysql://database-1.cxau8dx2g7op.ap-south-1.rds.amazonaws.com:3306/DEV").\
    option("user","myuser").\
    option("password","mypassword").\
    option("query","select * from emp where city='Pune'").\
    option("driver","com.mysql.cj.jdbc.Driver").\
    load()

#show data

df_emp.show()