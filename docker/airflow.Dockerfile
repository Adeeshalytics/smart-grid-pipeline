# Airflow image for the batch layer. It also carries PySpark, because the daily
# billing job runs with spark-submit in local mode inside the Airflow worker.
FROM apache/airflow:2.10.5-python3.11

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 TZ=UTC PYTHONPATH=/opt/grid/src

USER airflow
# boto3 already ships with the image (Amazon provider); listed so the build fails loudly if not.
RUN pip install --no-cache-dir pyspark==3.5.5 boto3

ARG MAVEN=https://repo1.maven.org/maven2
RUN set -eux; \
    JARS="$(python -c 'import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))')"; \
    for artifact in \
      org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
      com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
      org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar \
    ; do curl -fsSL --retry 5 -o "$JARS/$(basename "$artifact")" "$MAVEN/$artifact"; done

COPY --chown=airflow:root src /opt/grid/src
COPY --chown=airflow:root airflow/dags /opt/airflow/dags
