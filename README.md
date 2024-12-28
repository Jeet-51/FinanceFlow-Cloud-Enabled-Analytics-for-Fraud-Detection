# AWS-based-Financial-Data-Analysis-Pipeline

This project demonstrates an end-to-end solution for processing, analyzing, and predicting financial transaction patterns using AWS services and big data tools. The pipeline includes data ingestion, validation, transformation, machine learning, and visualization.

## Key Features:
- Data Ingestion: Raw financial data is ingested and stored securely in AWS S3.
- Data Processing: PySpark and Spark SQL are used for transformations and feature engineering.
- Machine Learning: Built a fraud detection model using AWS SageMaker Autopilot.
- Visualization: Interactive dashboards created with AWS QuickSight.

## Architecture
![image](https://github.com/user-attachments/assets/896b1903-61b9-415e-82d8-f0b6ee9fddf8)

## Technologies Used
- Cloud Services: AWS S3, SageMaker, SNS, QuickSight, EC2.
- Big Data Tools: PySpark, Spark SQL.
- Programming Language: Python.
- Visualization: AWS QuickSight
- Workflow Automation: AWS Step Functions.

## Dataset
Link to dataset - https://www.kaggle.com/datasets/ealaxi/paysim1

Data in S3 bucket
![image](https://github.com/user-attachments/assets/d395ec83-2dbe-4348-a4cf-53daf3fad28a)

## Results
- Fraud Detection F1 Score: 72.7%.
### Visual Insights:
- Monthly revenue trends show seasonal peaks in spending.
- Majority of revenue comes from high-value customers in specific regions.

## Challenges and Learnings
- Class Imbalance: Addressed through oversampling and class weighting techniques.
- Data Privacy: Ensured encryption and anonymization of PII.
- Real-Time Processing: Implemented AWS Kinesis for streaming use cases.

## Future Enhancements
- Add support for real-time fraud detection pipelines with AWS Kinesis.
- Explore advanced hyperparameter tuning methods for the SageMaker model.
- Incorporate more explainable AI tools like SHAP for better interpretability.
