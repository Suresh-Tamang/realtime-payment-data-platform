FROM apache/spark:3.5.0

# Switch to root to install jars
USER root

# Define the Postgres driver version
ENV PG_VERSION=42.7.3

# Download the Postgres JDBC driver into the Spark jars directory
ADD https://jdbc.postgresql.org/download/postgresql-${PG_VERSION}.jar /opt/spark/jars/postgresql-${PG_VERSION}.jar

# Set permissions so the spark user can read the jar
RUN chmod 644 /opt/spark/jars/postgresql-${PG_VERSION}.jar

# Switch back to the spark user
USER spark