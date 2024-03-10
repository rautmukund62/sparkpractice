from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark=SparkSession.builder.appName("Without header").getOrCreate()



header=StructType([
    StructField("eid",IntegerType(),True),
    StructField("ename",StringType(),True),
    StructField("did",IntegerType(),True),
    StructField("sal",IntegerType(),True),
    StructField("city",StringType(),True),
    StructField("__corrupt_record",StringType(),True)
])

#read
df=spark.read.format("CSV").option("inferschema","true")\
    .schema(header)\
    .option("mode","PERMISSIVE")\
    .option("columnNameOfCorruptRecord","__corrupt_record")\
    .load("D:/emp.csv")

df.show()

df_valid=df.filter(df['__corrupt_record'].isNull()).drop("__corrupt_record")

df_valid.show()

df_invalid=df.filter(df['__corrupt_record'].isNotNull())

df_invalid.show()