from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.master("local") \
    .appName("Word Count") \
   .getOrCreate()

# Read Excel file into DataFrame
df = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("file:///D:/emp.xlsx")

# Show the DataFrame
df.show()
