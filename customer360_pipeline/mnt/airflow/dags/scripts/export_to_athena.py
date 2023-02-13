import boto3
from airflow.models import Variable


def run_query(query, database, s3_output):
    client = boto3.client(
        "athena",
        aws_access_key_id=Variable.get("AWS_ACCESS_ID"),
        aws_secret_access_key=Variable.get("AWS_ACCESS_KEY"),
        region_name="ap-south-1",
    )
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={
            "OutputLocation": s3_output,
        },
    )
    print("Execution ID: " + response["QueryExecutionId"])
    return response


s3_input = "s3://orders-bucket-customer360/customer360_output/"
s3_output = "s3://orders-bucket-customer360/final_table_data/"
database = "airflow"
table = "final_table"
create_database = "CREATE DATABASE IF NOT EXISTS %s;" % (database)


create_table = f"""CREATE EXTERNAL TABLE IF NOT EXISTS `{database}`.`{table}` ( `customer_id` int, `customer_fname` string, `customer_lname` string, `order_id` int, `order_date` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ( 'separatorChar' = '\,', 'quoteChar' = '\\\'', 'escapeChar' = '\\\\'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '{s3_input}'
TBLPROPERTIES ('classification' = 'csv');"""

query_1 = "SELECT * FROM %s.%s where customer_id = 4;" % (database, table)
# Execute all queries
queries = [create_database, create_table, query_1]


def run_queries():

    for q in queries:
        print("Executing query: %s" % (q))
        run_query(q, database, s3_output)
