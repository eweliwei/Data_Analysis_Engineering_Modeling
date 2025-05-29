# Machine Learning Recruitment Test: Customer Segmentation with RFM Analysis

## Test Overview
This test evaluates candidates' abilities in exploratory data analysis, feature engineering, and unsupervised learning for customer segmentation using RFM (Recency, Frequency, Monetary) analysis.

## Dataset Description
You'll work with an orders dataset containing:

- `id`: Unique order identifier
- `created_at`: Timestamp of when the order was placed
- `sales_amount`: Monetary value of the order
- `customer_id`: Unique customer identifier

## Tasks
### 1. Data Loading and Understanding
- Load the dataset into your preferred analysis environment
- Perform initial data quality checks
- Provide basic statistics and distributions for numerical fields

### 2. Exploratory Data Analysis (EDA)
- Analyze the time span of the data
- Visualize order patterns
- Examine sales amount distribution
- Highlight any interesting patterns or anomalies

### 3. RFM Feature Engineering
Transform the order-level data into customer-level RFM features:
- Recency: Days since last purchase (reference date can be max date in data or today)
- Frequency: Number of orders placed
- Monetary Value: Total sales amount across all orders

### 4. Customer Segmentation
- Preprocess the RFM data
- Perform clustering (K-means or other appropriate algorithm)
- Justify your choice of number of clusters
- Describe your clustering approach
- Visualize the clusters

### 5. Cluster Analysis and Interpretation
- Analyze the characteristics of each cluster
- Provide meaningful labels for each segment
- Suggest business strategies for each customer segment

## Deliverables
- Clean, well-commented code (Python/R)
- Visualizations supporting your analysis
- Summary of findings
- Recommendations for business applications