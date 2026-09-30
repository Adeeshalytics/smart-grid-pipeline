# Spark image for the speed layer (Structured Streaming) and for running the tests.
FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1 PYTHONPATH=/opt/grid/src TZ=UTC \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl procps \
 && rm -rf /var/lib/apt/lists/*

COPY requirements/spark.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Connector jars are baked into the image so start-up needs no internet access:
# Kafka source/sink, S3A (object store) and the PostgreSQL JDBC driver.
ARG MAVEN=https://repo1.maven.org/maven2
RUN set -eux; \
    JARS="$(python -c 'import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))')"; \
    for artifact in \
      org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.5/spark-sql-kafka-0-10_2.12-3.5.5.jar \
      org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.5/spark-token-provider-kafka-0-10_2.12-3.5.5.jar \
      org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
      org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
      org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
      com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
      org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar \
    ; do curl -fsSL --retry 5 -o "$JARS/$(basename "$artifact")" "$MAVEN/$artifact"; done

WORKDIR /opt/grid
COPY src /opt/grid/src
COPY tests /opt/grid/tests
COPY pytest.ini /opt/grid/pytest.ini
