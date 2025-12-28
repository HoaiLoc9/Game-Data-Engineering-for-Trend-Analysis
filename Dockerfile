FROM apache/airflow:2.9.1-python3.11

USER root

# Cài OpenJDK 17 (mặc định trong Debian Bookworm)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        curl \
        ca-certificates && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME cho Java 17 (trên arm64 và amd64 đều giống nhau)
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
# Nếu máy bạn là Intel Mac (hiếm), đổi thành: /usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=${JAVA_HOME}/bin:${PATH}

# Cài Spark 3.5.3
ENV SPARK_VERSION=3.5.3
ENV HADOOP_VERSION=3
RUN curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz | \
    tar -xz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH=${SPARK_HOME}/bin:${PATH}

USER airflow