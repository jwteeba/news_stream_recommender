FROM apache/spark:3.5.0

USER root

RUN pip install --no-cache-dir \
    numpy \
    pandas \
    scikit-learn \
    nltk \
    pymongo \
    pyspark \
    wordcloud

# Define JAR versions
ARG SPARK_VERSION=3.5.0
ARG KAFKA_VERSION=3.4.0
ARG SCALA_VERSION=2.12
ARG COMMONS_POOL_VERSION=2.11.1
ARG MONGO_CONNECTOR_VERSION=3.0.1
ARG MONGO_DRIVER_VERSION=3.12.11

# Download JARs
RUN cd /opt/spark/jars && \
    wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_VERSION}/kafka-clients-${KAFKA_VERSION}.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/${COMMONS_POOL_VERSION}/commons-pool2-${COMMONS_POOL_VERSION}.jar && \
    wget -q https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_${SCALA_VERSION}/${MONGO_CONNECTOR_VERSION}/mongo-spark-connector_${SCALA_VERSION}-${MONGO_CONNECTOR_VERSION}.jar && \
    wget -q https://repo1.maven.org/maven2/org/mongodb/mongo-java-driver/${MONGO_DRIVER_VERSION}/mongo-java-driver-${MONGO_DRIVER_VERSION}.jar

USER spark
WORKDIR /app

COPY spark_streaming.py .

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD pgrep -f spark_streaming.py || exit 1

CMD ["python", "spark_streaming.py"]