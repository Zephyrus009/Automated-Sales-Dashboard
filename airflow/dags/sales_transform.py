from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

with DAG('Sales_ETL',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
    },
    schedule=timedelta(days=1),
    start_date=datetime(2022, 10, 21),
    catchup = False,


) as dag:
    
    # Sales Data task
    sales_data_transform = BashOperator(task_id ='sales_data_transform',
    bash_command='/opt/spark/bin/spark-submit /mnt/sales_data_job.py'
    )

    # Books Data task
    books_data_transform = BashOperator(task_id ='books_data_transform',
    bash_command='/opt/spark/bin/spark-submit /mnt/books_data_job.py'
    )

    sales_data_transform >> books_data_transform

