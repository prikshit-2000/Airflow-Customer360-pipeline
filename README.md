# customer360-pipeline

We will be implementing a pipeline that will be processing the customers and orders file and will provide the output to the customer service team for analysis and this output will be stored on AWS S3 also. 


## List of Components Used for the pipeline

VSCode Editor 🧑‍💻
Docker 🐳
Amazon S3 🪣
Hive 🐘
Spark 🌟
Airflow 💨
HDFS 📦
Gmail SMTP Server 📧
Slack 🔔

<i>*Note:- We will be using a Docker Contianer for Hive, Spark, HDFS and Airflow. So you just need docker for this 😉</i> 


## Pipeline implementation involves the below steps

Step 1: Checking if the orders file and customers is available in the S3 bucket

Step 2: Once the file is available , we are fetching the file from the Amazon S3 bucket to our local system

Step 4: Moving both customers and orders data to hdfs

Step 4.1: Processing orders data using Spark so we just have closed orders (This is an additional step you can skip if you want)

Step 5: creating customers and orders data in hive and loading it from hdfs

Step 6: Joining the customers and orders table using spark to get our final output

Step 7: Storing the final output to final_table and saving this output in local (you can store it in hdfs also if you want 🤗)

Step 8: Uploading the final output from local to AWS S3

Step7: Create Hbase TABLE


