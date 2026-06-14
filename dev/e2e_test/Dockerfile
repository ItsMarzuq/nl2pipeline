FROM apache/spark:3.5.1

# Switch to root to install Python packages
USER root

WORKDIR /app

# Install Python dependencies
COPY dev/e2e_test/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Download Spark packages at build time so runtime doesn't need internet
RUN /opt/spark/bin/spark-submit \
    --packages \
        org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,\
        com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 \
    --class org.apache.spark.examples.SparkPi \
    /opt/spark/examples/jars/spark-examples_2.12-3.5.1.jar 1 \
    > /dev/null 2>&1 || true

COPY dev/e2e_test/e2e_test.py .