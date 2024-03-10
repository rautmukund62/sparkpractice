from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("CSV Processing").getOrCreate()


#load

#Load
df=spark.read.format("CSV").option("header","true").option("inferschema","true").option("delimiter","|").load("D:/emp.txt")

#Process
df.createOrReplaceTempView("emp_tmp")
df1=spark.sql("select * from emp_tmp where sal > 30000")
df1.show()
df1.createOrReplaceTempView("emp1")
df2=spark.sql("select * from emp1 where did=10")
df2.show()

#Store
df2.write.format("CSV").option("header","true").option("delimiter",":").save("D:/empop1")