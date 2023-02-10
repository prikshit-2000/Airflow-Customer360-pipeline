
from os.path import expanduser, join, abspath

from pyspark.sql import SparkSession


warehouse_location = abspath('spark-warehouse')  
 
  
  # val sparkConf = new SparkConf();

  # sparkConf.set("spark.master","local[2]")
  
  
  # val spark = SparkSession \
  # .builder \
  # .config(sparkConf)
  # .getOrCreate()
  
spark = SparkSession \
  .builder \
  .appName("Customer360") \
  .config("spark.sql.warehouse.dir", warehouse_location) \
  .enableHiveSupport() \
  .getOrCreate()
  
ordersDf = spark \
.read \
.format("csv") \
.option("header", True) \
.option("inferSchema", True) \
.option("path", "hdfs://namenode:9000/airflow_input/orders.csv") \
.load()

#  ordersDf.show()
  
ordersDf.createTempView("orders")

ordersDf1=spark.sql("select * from orders where order_status='CLOSED'")

ordersDf1.show()

ordersDf1.write.format("csv").mode("overwrite").option("path","hdfs://namenode:9000/airflowoutput").save()  

