FROM apache/airflow:2.10.4

USER root
RUN apt-get update && apt-get install -y default-jdk && apt-get clean
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
USER airflow

COPY airflow/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt