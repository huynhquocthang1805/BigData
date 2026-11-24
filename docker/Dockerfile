# Một image dùng chung cho mọi service Python/Spark của dự án:
#   spark-master, spark-worker, stream-processor, producer, api, dashboard, batch jobs.
# Dùng chung image => driver và executor có CÙNG phiên bản Python/thư viện (bắt buộc với PySpark UDF/applyInPandas).
FROM eclipse-temurin:21-jre-noble

ARG SPARK_VERSION=4.2.1
ARG SCALA_BINARY=2.13
ARG KAFKA_CLIENTS_VERSION=3.9.2
ARG COMMONS_POOL2_VERSION=2.13.1
ARG POSTGRES_JDBC_VERSION=42.7.15

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 python3-venv python3-pip tini procps curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

RUN python3 -m venv /opt/venv
ENV PATH=/opt/venv/bin:$PATH \
    PYSPARK_PYTHON=/opt/venv/bin/python \
    PYSPARK_DRIVER_PYTHON=/opt/venv/bin/python

COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

# SPARK_HOME trỏ vào bản Spark đi kèm gói pip pyspark (có bin/spark-submit, bin/spark-class, jars/)
ENV SPARK_HOME=/opt/venv/lib/python3.12/site-packages/pyspark

# Connector Kafka cho Structured Streaming + JDBC PostgreSQL: tải sẵn lúc build (kiểm tra SHA1)
# => container chạy không cần tải gì từ Maven lúc runtime.
RUN set -eux; \
    M=https://repo1.maven.org/maven2; \
    for a in \
      "org/apache/spark/spark-sql-kafka-0-10_${SCALA_BINARY}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_BINARY}-${SPARK_VERSION}.jar" \
      "org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_BINARY}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_BINARY}-${SPARK_VERSION}.jar" \
      "org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar" \
      "org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar" \
      "org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar"; do \
      f="$SPARK_HOME/jars/$(basename "$a")"; \
      curl -fsSL "$M/$a" -o "$f"; \
      echo "$(curl -fsSL "$M/$a.sha1" | cut -c1-40)  $f" | sha1sum -c -; \
    done

WORKDIR /app
COPY . /app
ENV PYTHONPATH=/app \
    WQ_SPARK_JARS_BUNDLED=1

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "-c", "print('wq image: chỉ định command trong docker-compose.yml')"]
