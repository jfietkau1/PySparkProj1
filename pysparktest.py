from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, min, max, count

spark = SparkSession.builder.appName("CarDatasetAnalysis").getOrCreate()

df = spark.read.csv("car_price_dataset.csv", header=True, inferSchema=True)

df.show(5)



# Find the most and least expensive cars
most_expensive = df.orderBy(col("Price").desc()).select("Brand", "Model", "Price").first()
least_expensive = df.orderBy(col("Price").asc()).select("Brand", "Model", "Price").first()

print(f"Most Expensive Car: {most_expensive}")
print(f"Least Expensive Car: {least_expensive}")

# Count the number of cars per brand
brand_counts = df.groupBy("Brand").agg(count("*").alias("Count"))
brand_counts.show()

# Compute average, min, and max mileage
mileage_stats = df.agg(
    avg("Mileage").alias("Avg_Mileage"),
    min("Mileage").alias("Min_Mileage"),
    max("Mileage").alias("Max_Mileage")
)

mileage_stats.show()


