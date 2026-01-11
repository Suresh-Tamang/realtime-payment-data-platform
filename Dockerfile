FROM apache/spark:3.5.0

# Switch to root to perform installations and config changes
USER root

# 1. Define Versions
ENV PG_VERSION=42.7.3
ENV KAFKA_PACKAGE="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"

# 2. Download the Postgres JDBC driver directly into Spark's default jar folder
# This ensures it is available to both the Master and the Worker automatically.
ADD https://jdbc.postgresql.org/download/postgresql-${PG_VERSION}.jar /opt/spark/jars/postgresql-${PG_VERSION}.jar
RUN chmod 644 /opt/spark/jars/postgresql-${PG_VERSION}.jar

# 3. Pre-cache the Kafka package to speed up job startup
RUN /opt/spark/bin/spark-submit --packages ${KAFKA_PACKAGE} --version

# 4. Configure Spark Defaults
# We set the Kafka package and Ivy cache paths here so they don't have to be passed via command line every time.
RUN mkdir -p /opt/spark/conf && \
    echo "spark.jars.packages ${KAFKA_PACKAGE}" >> /opt/spark/conf/spark-defaults.conf && \
    echo "spark.driver.extraJavaOptions -Divy.cache.dir=/tmp -Divy.home=/tmp" >> /opt/spark/conf/spark-defaults.conf && \
    echo "spark.executor.extraJavaOptions -Divy.cache.dir=/tmp -Divy.home=/tmp" >> /opt/spark/conf/spark-defaults.conf

# 5. Prepare the data directory
# This is crucial so that the 'spark' user has permission to write the CSV files
RUN mkdir -p /opt/spark-data && chown -R spark:spark /opt/spark-data /opt/spark/conf

# Switch back to the non-root spark user for security
USER spark