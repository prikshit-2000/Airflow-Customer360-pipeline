import boto3
from airflow import DAG
from airflow import settings
from airflow.models import Variable
from airflow.models.connection import Connection
from airflow.utils.dates import days_ago

from airflow.sensors.http_sensor import HttpSensor
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.email import EmailOperator

from files.variables import airflow_variables
from scripts.export_to_postgres import run_queries

for key, value in airflow_variables.items():
    Variable.set(key, value=value)


def local_to_S3():
    access_id = Variable.get("AWS_ACCESS_ID")
    access_key = Variable.get("AWS_ACCESS_KEY")
    s3 = boto3.resource(
        "s3", aws_access_key_id=access_id, aws_secret_access_key=access_key
    )
    BUCKET = "orders-bucket-customer360"
    s3.Bucket(BUCKET).upload_file(
        "/tmp/customer360_output/customer360_final.csv",
        "customer360_output/customer360_final.csv",
    )


def get_order_url():
    session = settings.Session()
    conn = session.query(Connection).filter(Connection.conn_id == "order_s3").first()

    conn.host
    return f"{conn.schema}://{conn.host}orders.csv"


def get_customer_url():
    session = settings.Session()
    conn = session.query(Connection).filter(Connection.conn_id == "order_s3").first()

    conn.host
    return f"{conn.schema}://{conn.host}customers.csv"


def get_order_filter_cmd():
    command_zero = "export SPARK_MAJOR_VERSION=2"
    command_one = "hdfs dfs -rm -R -f airflowoutput_orders"
    return f"{command_zero} && {command_one}"


def create_order_hive_table_cmd():
    command_one = 'hive -e "CREATE DATABASE IF NOT EXISTS airflow_final"'
    command_two = "hive -e \"create external table if not exists airflow_final.orders(order_id int,order_date string,order_customer_id int,status string) row format delimited fields terminated by ',' stored as textfile location '/airflowoutput_orders'\""
    return f"{command_one} && {command_two}"


def create_customer_hive_table_cmd():
    command_one = 'hive -e "CREATE DATABASE IF NOT EXISTS airflow_final"'
    command_two = "hive -e \"create  table if not exists airflow_final.customers(customer_id int,customer_fname string,customer_lname string,customer_email string,customer_password string,customer_street string,customer_city string,customer_state string,customer_zipcode int) row format delimited fields terminated by ','\" "
    command_three = "hive -e \"LOAD DATA  INPATH '/airflow_input/customers.csv' INTO TABLE airflow_final.customers\"";
    return f"{command_one} && {command_two} && {command_three}"


def create_final_table_cmd():
    command_one = "hive -e \"create  table if not exists airflow_final.final_table(customer_id int,customer_fname string,customer_lname string,order_id int,order_date string) row format delimited fields terminated by ','\""

    return f"{command_one}"


def hive_to_csv():
    command_one = (
        "cd /tmp && rm -rf customer360_output && mkdir -p customer360_output && cd .."
    )
    
    command_three = """hive -e "select * from airflow_final.final_table" |  sed 's/[\t]/,/g'  > /tmp/customer360_output/customer360_final.csv"""
    return f"{command_one} && {command_three}"


dag = DAG(dag_id="customer_360_pipeline", start_date=days_ago(1))

sensor = HttpSensor(
    task_id="watch_for_orders",
    http_conn_id="order_s3",
    endpoint="orders.csv",
    response_check=lambda response: response.status_code == 200,
    dag=dag,
)
sensor2 = HttpSensor(
    task_id="watch_for_customers",
    http_conn_id="order_s3",
    endpoint="customers.csv",
    response_check=lambda response: response.status_code == 200,
    dag=dag,
)


download_order_cmd = f"cd /tmp && rm -rf airflow_pipeline && mkdir -p airflow_pipeline && cd airflow_pipeline && wget {get_order_url()} && wget {get_customer_url()}"


download_to_edgenode = BashOperator(
    task_id="download_data", bash_command=download_order_cmd, dag=dag
)


upload_order_info = BashOperator(
    task_id="upload_orders_to_hdfs",
    bash_command="hdfs dfs -rm -R -f airflow_input && hdfs dfs -mkdir -p /airflow_input && hdfs dfs -put -f  /tmp/airflow_pipeline/orders.csv /airflow_input",
    dag=dag,
)

upload_customer_info = BashOperator(
    task_id="upload_customer_to_hdfs",
    bash_command="hdfs dfs -rm -R -f airflow_input && hdfs dfs -mkdir -p /airflow_input && hdfs dfs -put -f  /tmp/airflow_pipeline/customers.csv /airflow_input",
    dag=dag,
)


process_order_info = BashOperator(
    task_id="process_orders", bash_command=get_order_filter_cmd(), dag=dag
)
spark_job = SparkSubmitOperator(
    task_id="spark_job",
    conn_id="spark_conn",
    application="/opt/airflow/dags/scripts/process_orders.py",
    verbose=False,
)


create_order_table = BashOperator(
    task_id="create_orders_table_hive",
    bash_command=create_order_hive_table_cmd(),
    dag=dag,
)
create_customer_table = BashOperator(
    task_id="create_customers_table_hive",
    bash_command=create_customer_hive_table_cmd(),
    dag=dag,
)


create_final_table = BashOperator(
    task_id="create_final_table", bash_command=create_final_table_cmd(), dag=dag
)


final_table = SparkSubmitOperator(
    task_id="final_table",
    conn_id="spark_conn",
    application="/opt/airflow/dags/scripts/final_table.py",
    verbose=False,
)


final_hive_table = BashOperator(
    task_id="final_hive_table", bash_command=hive_to_csv(), dag=dag
)

export_to_S3 = PythonOperator(
    task_id="export_to_s3",
    python_callable=local_to_S3,
)

export_to_postgres = PythonOperator(
    task_id="export_to_athena",
    python_callable=run_queries,
)

send_to_email = Variable.get("Send_to_email")

email_success = "https://" + Variable.get("bucket_url") + "images/email_success.jpg"
email_fail = "https://" + Variable.get("bucket_url") + "images/email_fail.jpg"

send_email_success = EmailOperator(
    task_id="send_email_success",
    to=send_to_email,
    subject="customer360 pipeline success!",
    html_content=f"""
    <h3>Congratulations! Data loaded in S3 bucket successfully.</h3>
    <img src = {email_success}></img>
    """,
    trigger_rule="all_success",
    dag=dag,
)

send_email_failed = EmailOperator(
    task_id="send_email_failed",
    to=send_to_email,
    subject="customer360 pipeline failed!",
    html_content=f"""
    <h3>Sorry! Data loading failed in S3 bucket.</h3>
     <img src = {email_fail}></img>
    """,
    trigger_rule="all_failed",
    dag=dag,
)


slack_notify_success = SlackWebhookOperator(
    task_id="slack_notification_success",
    http_conn_id="slack_conn",
    message="Congratulations! Data loaded in S3 bucket successfully.",
    channel="#notifications",
    dag=dag,
)

slack_notify_fail = SlackWebhookOperator(
    task_id="slack_notification_fail",
    http_conn_id="slack_conn",
    message="Sorry! Data loading failed in S3 bucket.",
    channel="#notifications",
    dag=dag,
)


[sensor, sensor2] >> download_to_edgenode >> [upload_order_info,upload_customer_info]

upload_order_info >> process_order_info >> spark_job >> create_order_table
upload_customer_info >> create_customer_table
(
    [create_order_table, create_customer_table]
    >> create_final_table
    >> final_table
    >> final_hive_table
)
final_hive_table >> export_to_S3 >> export_to_postgres >>[send_email_success, send_email_failed]
send_email_success >> slack_notify_success
send_email_failed >> slack_notify_fail
