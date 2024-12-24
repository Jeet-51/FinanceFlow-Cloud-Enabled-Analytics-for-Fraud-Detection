from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("DataTransformation") \
    .config("spark.jars", "/home/ec2-user/spark_jars/hadoop-aws-3.3.4.jar,/home/ec2-user/spark_jars/aws-java-sdk-bundle-1.12.524.jar") \
    .config("spark.hadoop.fs.s3a.access.key", "ACCESS_ID") \
    .config("spark.hadoop.fs.s3a.secret.key", "SECRET_KEY") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

# Load Raw Data
bucket_name = "patejeetbigdata"
file_key = "bank_transactions.csv"
input_path = f"s3a://{bucket_name}/{file_key}"

print("Loading raw data from S3...")
raw_data = spark.read.csv(input_path, header=True, inferSchema=True)

# Show Original Data
print("Original Data Schema:")
raw_data.printSchema()

# Step 1: Split TransactionDate into Day, Month, and Year
split_date = split(col("TransactionDate"), "/")

# Add Year and Month to the DataFrame while retaining all original columns
transformed_data = raw_data \
    .withColumn("Day", split_date.getItem(0)) \
    .withColumn("Month", split_date.getItem(1)) \
    .withColumn("Year", split_date.getItem(2))

# Step 2: Inspect Transformed Data
print("Transformed Data Schema (Original + Year/Month):")
transformed_data.printSchema()

print("Sample Rows of Transformed Data:")
transformed_data.show(10, truncate=False)  # Show first 10 rows without truncating

# Step 3: Save Transformed Data Back to S3
output_path = f"s3a://{bucket_name}/intermediate/transformed_data/"
print("Saving transformed data to S3...")
transformed_data.write.csv(output_path, header=True, mode="overwrite")

print("Data transformation complete.")
