
from os.path import expanduser, join, abspath

from pyspark.sql import SparkSession
from pyspark.sql import Row
import pyspark


warehouse_location = abspath('spark-warehouse')  
 
  
  # val sparkConf = new SparkConf();

  # sparkConf.set("spark.master","local[2]")
  
  
  # val spark = SparkSession \
  # .builder \
  # .config(sparkConf)
  # .getOrCreate()
  
spark = SparkSession \
  .builder \
  .appName("final_table") \
  .config("spark.sql.warehouse.dir", warehouse_location) \
  .enableHiveSupport() \
  .getOrCreate()
  
df_customers = spark.sql("select * from airflow_final.customers")
df_orders = spark.sql("select * from airflow_final.orders")

df_customers.show()
df_orders.show()


df_final = spark.sql(" select c.customer_id,c.customer_fname,c.customer_lname,o.order_id,o.order_date from airflow_final.customers c join airflow_final.orders o on (c.customer_id=o.customer_id)")
# df_final.write.format("csv").mode("overwrite").option("path","").save()
df_final.write.insertInto('airflow_final.final_table',overwrite = False)
df = spark.sql("select * from airflow_final.final_table")
df.show()