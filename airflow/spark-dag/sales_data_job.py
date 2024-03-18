import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType
from pyspark.sql import functions as F
from datetime import datetime

spark = SparkSession.builder.appName('Sales_Pipeline').getOrCreate()

def write_to_postgres(df, url, table):
    df.write \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", table) \
        .option("user", "[YOUR USER]") \
        .option("password", "[YOUR PASSWORD]") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
    print("Sparkframe Pushed Successfully")

schema = StructType([
    StructField("Order ID",StringType(),False),
    StructField("Product",StringType(),True),
    StructField("Quantity Ordered",IntegerType(),True),
    StructField("Price Each",FloatType(),True),
    StructField("Order Date",StringType(),True),
    StructField("Purchase Address",StringType(),True),
])

sales_data = spark.read.format('csv').option('header',True).schema(schema).load('/opt/airflow/disk_data/raw')

## Sales Data format conversion
sales_data = sales_data.withColumn('Order Date', F.to_date(F.unix_timestamp(F.col('Order Date'), 'MM/dd/yy HH:mm').cast("timestamp")))
sales_data = sales_data.orderBy(['Order Date'],ascending = [False])
sales_data = sales_data.withColumn('created_on',F.current_timestamp().cast('timestamp'))
sales_data = sales_data.withColumn('changes_on',sales_data['created_on'])

new_names = ['order_id','product','quantity','price','order_date','purchase_address']
old_names = list(sales_data.columns)[:-2]
for old_name, new_name in zip(old_names,new_names):
    sales_data = sales_data.withColumnRenamed(old_name, new_name)


url = "jdbc:postgresql://aws-0-ap-south-1.pooler.supabase.com:5432/postgres"
table = "product_sales_data"

write_to_postgres(sales_data,url,table)