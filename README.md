# Apache-Spark-Airflow


This is a Implementation of Fecting data from a soucre, pass the raw data through a data pipeline or ETL and save it to a Database. After that fetching it from there and updating the dashboard in apache superset

### Here we are using 

1. Database source - Supabase (postgresql)
2. Transformation Tool - Apache Spark
3. Orchestation Tool - Apache Airflow
4. visualization tool - Apache Superset

## Steps to Initialize the project

1. Clone the github repository

```bash
git clone https://github.com/Zephyrus009/Automated-Sales-Dashboard.git
```

2. Move to spark-dag folder where you can place your spark jobs.

    *** please change [YOUR USER] and [YOUR PASSWORD] in write_to_postgres function

3. Now build the Docker 

```bash
docker build . -t spark-airflow-postgres:latest
```

4. Now Pull the required images and up the compose file

```bash
docker compose up
```

5. You can access the Airflow in http://localhost:8080/

6. Also you can access the superset from the here - [Apache Superset](https://github.com/apache/superset) to create interactive dashboards


