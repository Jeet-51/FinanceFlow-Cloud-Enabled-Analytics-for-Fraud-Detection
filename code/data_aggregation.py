from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, max, desc

# Step 1: Initialize Spark Session
spark = SparkSession.builder \
    .appName("DataAggregation") \
    .config("spark.jars", "/home/ec2-user/spark_jars/hadoop-aws-3.3.4.jar,/home/ec2-user/spark_jars/aws-java-sdk-bundle-1.12.524.jar") \
    .config("spark.hadoop.fs.s3a.access.key", "AKIAYKFQRAWCMOFVFOMT") \
    .config("spark.hadoop.fs.s3a.secret.key", "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") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

# Step 2: Load Transformed Data
bucket_name = "patejeetbigdata"
input_path = f"s3a://{bucket_name}/intermediate/transformed_data/"
print("Loading transformed data from S3...")
data = spark.read.csv(input_path, header=True, inferSchema=True)

# Show the schema of the data to verify column names
print("Schema of Loaded Data:")
data.printSchema()

# Step 3: Compute Aggregated Metrics

# 1. Total Revenue by Region
print("Computing Total Revenue by Region...")
total_revenue_by_region = data.groupBy("CustLocation").agg(
    sum(col("`TransactionAmount (INR)`")).alias("TotalRevenue")
)
total_revenue_by_region.show()

# 2. Monthly Spending Trends
print("Computing Monthly Spending Trends...")
monthly_spending_trends = data.groupBy("Year", "Month").agg(
    sum(col("`TransactionAmount (INR)`")).alias("MonthlySpending")
)
monthly_spending_trends.show()

# 3. Top 10 Customers by Transaction Value
print("Identifying Top 10 Customers by Transaction Value...")
top_customers = data.groupBy("CustomerID").agg(
    sum(col("`TransactionAmount (INR)`")).alias("TotalTransactionValue")
).orderBy(desc("TotalTransactionValue")).limit(10)
top_customers.show()

# 4. Average Transaction Amount by Region
print("Computing Average Transaction Amount by Region...")
avg_transaction_by_region = data.groupBy("CustLocation").agg(
    avg(col("`TransactionAmount (INR)`")).alias("AvgTransactionAmount")
)
avg_transaction_by_region.show()

# 5. Maximum Transaction Amount by Region
print("Computing Maximum Transaction Amount by Region...")
max_transaction_by_region = data.groupBy("CustLocation").agg(
    max(col("`TransactionAmount (INR)`")).alias("MaxTransactionAmount")
)
max_transaction_by_region.show()

# Step 4: Save Aggregated Results Back to S3
output_path = f"s3a://{bucket_name}/aggregated_results/"
print("Saving aggregated results to S3...")

# Save each result to a separate folder in S3
total_revenue_by_region.write.csv(output_path + "total_revenue_by_region", header=True, mode="overwrite")
monthly_spending_trends.write.csv(output_path + "monthly_spending_trends", header=True, mode="overwrite")
top_customers.write.csv(output_path + "top_10_customers", header=True, mode="overwrite")
avg_transaction_by_region.write.csv(output_path + "avg_transaction_by_region", header=True, mode="overwrite")
max_transaction_by_region.write.csv(output_path + "max_transaction_by_region", header=True, mode="overwrite")

print("Data aggregation complete.")
