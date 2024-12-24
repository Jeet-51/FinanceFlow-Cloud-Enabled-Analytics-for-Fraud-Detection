from pyspark.sql import SparkSession

# Step 1: Initialize Spark Session
spark = SparkSession.builder \
    .appName("DataIngestionFromS3") \
    .config("spark.jars", "hadoop-aws-3.3.4.jar, aws-java-sdk-bundle-1.12.524.jar") \
    .config("spark.hadoop.fs.s3a.access.key", "ACCESS_ID") \
    .config("spark.hadoop.fs.s3a.secret.key", "SECRET_KEY") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

# Step 2: Define S3 Path
bucket_name = "patejeetbigdata"  # Replace with your bucket name
file_key = "bank_transactions.csv"  # Replace with your file name
input_path = f"s3a://{bucket_name}/{file_key}"

# Step 3: Load Data from S3
print("Loading raw data from S3...")
raw_data = spark.read.csv(input_path, header=True, inferSchema=True)

# Step 4: Inspect the Dataset
print("Schema of the dataset:")
raw_data.printSchema()

print("Sample data:")
raw_data.show(5)
