FROM apache/airflow:3.3.0-python3.12

USER root

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

# Force Spark/PySpark à utiliser Python 3.12
ENV PYSPARK_PYTHON=/usr/local/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps \
        curl && \
    rm -rf /var/lib/apt/lists/*

# Récupère Spark 3.5.1 depuis l'image officielle
COPY --from=apache/spark:3.5.1 /opt/spark /opt/spark

# Dépendances S3 / MinIO pour Spark
RUN curl -L \
    -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L \
    -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark \
    duckdb \
    minio \
    python-dotenv \
    kagglehub \
    pymongo
