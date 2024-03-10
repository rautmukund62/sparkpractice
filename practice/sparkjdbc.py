from pyspark.sql import  SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("RDBMS data process").getOrCreate()

#read data for emp table
df_emp=spark.read.format("JDBC").option("url","jdbc:mysql://database-1.cxau8dx2g7op.ap-south-1.rds.amazonaws.com:3306/DEV").\
    option("user","myuser").\
    option("password","mypassword").\
    option("dbtable","emp").\
    option("driver","com.mysql.cj.jdbc.Driver").\
    load()

#read data for dept table
df_dept=spark.read.format("JDBC").option("url","jdbc:mysql://database-1.cxau8dx2g7op.ap-south-1.rds.amazonaws.com:3306/DEV").\
    option("user","myuser").\
    option("password","mypassword").\
    option("dbtable","dept").\
    option("driver","com.mysql.cj.jdbc.Driver").\
    load()

df_emp.createOrReplaceTempView("e")
df_dept.createOrReplaceTempView("d")


df=spark.sql("select e.*,d.dname from e inner join d on e.did=d.did")

#writing in RDBMS
df.write.format("JDBC").option("url","jdbc:mysql://database-1.cxau8dx2g7op.ap-south-1.rds.amazonaws.com:3306/DEV").\
    option("user","myuser").\
    option("password","mypassword").\
    option("dbtable","emp_dept").\
    option("driver","com.mysql.cj.jdbc.Driver").\
    save()