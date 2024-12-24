from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Step 1: Initialize Spark Session
spark = SparkSession.builder \
    .appName("DataAnalysisUsingSparkSQL") \
    .config("spark.jars", "/home/ec2-user/spark_jars/hadoop-aws-3.3.4.jar,/home/ec2-user/spark_jars/aws-java-sdk-bundle-1.12.524.jar") \
    .config("spark.hadoop.fs.s3a.access.key", "AKIAYKFQRAWCMOFVFOMT") \
    .config("spark.hadoop.fs.s3a.secret.key", "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") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

# Step 2: Load Preprocessed Transactions Data from S3
bucket_name = "patejeetbigdata"
data_path = f"s3a://{bucket_name}/preprocessed_transactions.csv"
print("Loading preprocessed transactions data from S3...")

# Read the data into a DataFrame
data = spark.read.csv(data_path, header=True, inferSchema=True)

# Display schema for verification
print("Schema of Preprocessed Transactions Data:")
data.printSchema()

# Step 3: Register DataFrame as a Temporary SQL Table
data.createOrReplaceTempView("preprocessed_transactions")

# Step 4: Run SQL Queries to Derive Insights

# 1. Top-Performing Regions by Total Revenue
print("Query 1: Top-Performing Regions by Total Revenue")
top_regions = spark.sql("""
    SELECT CustLocation, SUM(`TransactionAmount (INR)`) AS TotalRevenue
    FROM preprocessed_transactions
    GROUP BY CustLocation
    ORDER BY TotalRevenue DESC
""")
top_regions.show()

# 2. Monthly Revenue Trends
print("Query 2: Monthly Revenue Trends")
monthly_revenue = spark.sql("""
    SELECT Year, Month, SUM(`TransactionAmount (INR)`) AS MonthlyRevenue
    FROM preprocessed_transactions
    GROUP BY Year, Month
    ORDER BY Year, Month
""")
monthly_revenue.show()

# 3. Top 10 Customers by Total Transaction Value
print("Query 3: Top 10 Customers by Total Transaction Value")
top_customers = spark.sql("""
    SELECT CustomerID, SUM(`TransactionAmount (INR)`) AS TotalTransactionValue
    FROM preprocessed_transactions
    GROUP BY CustomerID
    ORDER BY TotalTransactionValue DESC
    LIMIT 10
""")
top_customers.show()

# 4. Average Transaction Value per Region
print("Query 4: Average Transaction Value by Region")
avg_transaction_region = spark.sql("""
    SELECT CustLocation, AVG(`TransactionAmount (INR)`) AS AvgTransactionValue
    FROM preprocessed_transactions
    GROUP BY CustLocation
    ORDER BY AvgTransactionValue DESC
""")
avg_transaction_region.show()

# 5. Total Number of Transactions per Customer
print("Query 5: Total Transactions per Customer")
total_transactions_customer = spark.sql("""
    SELECT CustomerID, COUNT(*) AS TotalTransactions
    FROM preprocessed_transactions
    GROUP BY CustomerID
    ORDER BY TotalTransactions DESC
""")
total_transactions_customer.show()

# Step 5: Save Query Results Back to S3
output_path = f"s3a://{bucket_name}/analysis_results/"
print("Saving SQL query results to S3...")

# Save all query results as CSV files
top_regions.write.mode("overwrite").csv(output_path + "top_performing_regions", header=True)
monthly_revenue.write.mode("overwrite").csv(output_path + "monthly_revenue_trends", header=True)
top_customers.write.mode("overwrite").csv(output_path + "top_customers", header=True)
avg_transaction_region.write.mode("overwrite").csv(output_path + "avg_transaction_region", header=True)
total_transactions_customer.write.mode("overwrite").csv(output_path + "total_transactions_customer", header=True)

print("Task 4 complete: Results saved to S3.")
