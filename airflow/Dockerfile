
FROM apache/airflow:2.6.2

USER root

ARG SPARK_VERSION=3.3.0 \
HADOOP_VERSION=3
# Note: this is needed when you use Python 3.3 or greater
ENV SPARK_HOME=/opt/spark \
SPARK_OPTS="--driver-java-options=-Dlog4j.logLevel=info" \
SPARK_VERSION=${SPARK_VERSION} \
HADOOP_VERSION=${HADOOP_VERSION} \
PYTHONHASHSEED=1

RUN set -ex && \
    apt-get update && \
    apt-get install -yqq openssh-client wget openjdk-11-jdk && \
    mkdir /opt/spark && \
    mkdir /opt/spark/jars && \
    wget --no-verbose -O apache-spark.tgz "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
    tar -xf apache-spark.tgz -C /opt/spark --strip-components=1 && \
    rm apache-spark.tgz && \
    wget --no-verbose -O /opt/spark/jars/postgresql-42.5.0.jar "https://jdbc.postgresql.org/download/postgresql-42.5.0.jar"
    

# Specify the User that the actual main process will run as
ARG AIRFLOW_UID="50000"
USER ${AIRFLOW_UID}
