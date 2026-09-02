FROM apache/spark:3.5.1

USER root

RUN curl -L \
    -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

RUN pip install --no-cache-dir \
    python-dotenv

USER spark