# Customer360 Pipeline

We will be implementing a pipeline that will be processing the customers and orders file and will provide the output to the customer service team for analysis and this output will be stored on AWS S3 also. 

<img src = "/images/template.png"></img>

## List of Components Used for the pipeline

- VSCode Editor 🧑‍💻
- Docker 🐳
- Amazon S3 🪣
- Hive 🐘
- Spark 🌟
- Airflow 💨
- HDFS 📦
- Gmail SMTP Server 📧
- Slack 🔔

<i>Note*:- We will be using a Docker Contianer for Hive, Spark, HDFS and Airflow. So you just need docker for this 😉</i> 


## Pipeline implementation involves the below steps

Step 1: Checking if the orders file and customers is available in the S3 bucket

Step 2: Once the file is available , we are fetching the file from the Amazon S3 bucket to our local system

Step 3: Moving both customers and orders data to hdfs

Step 3.1: Processing orders data using Spark so we just have closed orders (This is an additional step you can skip if you want)

Step 4: creating customers and orders data in hive and loading it from hdfs

Step 5: Joining the customers and orders table using spark to get our final output

Step 6: Storing the final output to final_table and saving this output in local (you can store it in hdfs also if you want 🤗)

Step 7: Uploading the final output from local to AWS S3

Step 8: Creating the table in AWS Athena and querying it in AWS Athena

### Connections

#### order_s3 connection to fetch data from aws s3 bucket

<img src = "/images/aws_conn.png"></img>

#### spark connection in airflow

<img src = "/images/spark_conn.png"></img>

#### slack connection in airflow

<img src = "/images/slack_conn.png"></img>

### Requirements
- AWS free account
- Docker should be installed

### Execution Steps

- Clone the repo and your current working directory should be customer360_pipeline
- Run start.sh file inside customer360_pipeline folder using command bash start.sh (for windows) / ./start.sh  to start the required docker containers needed for the project 
- if your containers are healthy and running you will be able to navigate to airlfow UI (localhost:8080)
- Make the above connections there and the variables will be made by our customer360.py (inside dag folder) automatically.
- Just go to the UI now and start the dag.

### Customer 360 DAG
<img src = "/images/airflow_dag.png"></img>

### Gmail alerts
Pipeline Success!<br>
<img src = "/images/pipeline_success_email.png" height="200" width="200"></img>
<br>
Pipeline Failed!<br>
<img src = "/images/pipeline_failed_email.png" height="200" width="200"></img>
<br>
### Slack alerts
Pipeline Success!<br>
<img src = "/images/slack_success.png" height="100" width="1000"></img>
<br>
Pipeline Failed!<br>
<img src = "/images/slack_fail.png" height="100" width="1000"></img>
<br>

