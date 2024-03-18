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

books_data = spark.read.format('csv').option('header',True).option('InferedSchema',True).load('/opt/airflow/disk_data/books_data.csv')
books_rating = spark.read.format('csv').option('header',True).option('InferedSchema',True).load('/opt/airflow/disk_data/Books_Rating.csv')

# handling books data

books_data = books_data.filter((books_data.authors.startswith('[')) & books_data.authors.endswith(']'))
books_data = books_data.filter((books_data.categories.startswith('[')) & books_data.categories.endswith(']'))

books_data = books_data.withColumn("authors",F.regexp_replace("authors","\['",""))
books_data = books_data.withColumn("authors",F.regexp_replace("authors","\']",""))
books_data = books_data.withColumn("categories",F.regexp_replace("authors","\['",""))
books_data = books_data.withColumn("categories",F.regexp_replace("authors","\']",""))

books_data = books_data.select('Title','authors','categories')
books_data = books_data.withColumnRenamed('Title','Title_upt')

## handling books rating

books_rating = books_rating.select('Id','Title','review/score','Price')

## Joining the required data
books_data = books_rating.join(books_data,books_rating['Title'] == books_data['Title_upt'],'left')

## formatting data
books_data = books_data.filter(books_data.Title_upt != 'NULL')
books_data = books_data.filter(books_data.Price != 'NULL')
books_data = books_data.drop('Title_upt')

## renaming columns
current_columns = ['Id', 'Title', 'Price', 'review/score', 'authors', 'categories']
new_columns = ['id', 'title', 'price', 'review_score', 'authors', 'categories']

for current_column,new_column in zip(current_columns,new_columns):
    books_data = books_data.withColumnRenamed(current_column,new_column )

## Typecasting data
books_data = books_data.withColumn("price",F.col("price").cast("float"))
books_data = books_data.withColumn("review_score",F.col("review_score").cast("float"))

## Pushing to Database

url = "jdbc:postgresql://aws-0-ap-south-1.pooler.supabase.com:5432/postgres"
table = "book_sales_data"

write_to_postgres(books_data,url,table)
