
# Use the official lightweight Apache Spark image with PySpark
FROM apache/spark-py:v3.3.2

# Set environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$PATH"

# Switch to root user to install dependencies
USER root

# Install Python and necessary dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*  # Clean up to reduce image size

# Upgrade pip and install PySpark (already included in the base image, but ensuring latest version)
# with other necessary packages
RUN python3 -m pip install --no-cache-dir --upgrade pip pyspark pytest py4j cryptography 
RUN pip install python-dotenv fsspec hdfs3

# Create main directory for the ingestion service
RUN mkdir -p /app 

# Set working directory
WORKDIR /app/ingestion

#set the python path
ENV PYTHONPATH=/app

#Copy the content of spark_ingestion folder
COPY . /app/ingestion


# Set proper permissions
RUN chmod -R 777 /app

# Debugging Step: Print installed Python packages
RUN pip3 freeze > /app/installed_packages.txt
RUN cat /app/installed_packages.txt

# Run a test command to confirm pyspark is installed
RUN python3 -c "import pyspark; print('Pyspark is installed successfully')" || echo "Pyspark import failed!"

# Command to run the ingestion script
CMD ["/opt/spark/bin/spark-submit", "--master", "spark://spark-master:7077", "--conf", "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:/app/conf/log4j.properties", "--conf", "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:/app/conf/log4j.properties", "/app/ingestion/ingestion.py"]