from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, rand

# Create a SparkSession
spark = SparkSession.builder \
    .appName("DataSkewnessExample") \
    .getOrCreate()

# Read data from CSV file
df = spark.read.format("CSV").option("header","true").load("input.csv")

# Define the number of salt partitions
num_salts = 10

# Add salt to mitigate skewness
salted_df = df.withColumn("salt", (rand() * num_salts).cast("int").cast("string"))
salted_df = salted_df.withColumn("salted_key", concat(col("key"), lit("_"), col("salt")))

# Group by salted_key to distribute data more evenly
result = salted_df.groupBy("salted_key").count().orderBy("salted_key")

# Show the result
result.show()

# Stop the SparkSession
spark.stop()
