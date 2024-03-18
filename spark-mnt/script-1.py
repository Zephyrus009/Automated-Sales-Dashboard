from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

laureate_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://spark-airflow_postgres_1:5432/airflow") \
    .option("dbtable", "airflow.public.laureate") \
    .option("user", "airflow") \
    .option("password", "airflow") \
    .option("driver", "org.postgresql.Driver") \
    .load()

# Get list of top 5 countries where nobel laureates were born
top_5_countries_df = laureate_df.filter(col("borncountry")!="null").groupBy("borncountry").count()
top_5_countries_df.sort("count",ascending=False).limit(5) \
    .write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://spark-airflow_postgres_1:5432/airflow") \
    .option("dbtable", "airflow.public.top_5_countries") \
    .option("user", "airflow") \
    .option("password", "airflow") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()