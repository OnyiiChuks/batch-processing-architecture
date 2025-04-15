FROM debian:bullseye-slim 

# Install system dependencies
RUN apt-get update && apt-get install -y \
    default-jdk \
    curl \
    wget \
    gcc \
    libpq-dev 
    #\
    #&& apt-get clean

# Set JAVA_HOME
RUN apt-get update && apt-get install -y openjdk-11-jdk
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Install Spark with Hadoop and PostgreSQL driver
RUN wget https://archive.apache.org/dist/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz && \
    tar -xvzf spark-3.1.2-bin-hadoop3.2.tgz && \
    mv spark-3.1.2-bin-hadoop3.2 /opt/spark && \
    rm spark-3.1.2-bin-hadoop3.2.tgz

# Download the PostgreSQL JDBC driver
RUN wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar -P /opt/spark/jars


# Set Spark and Java environment
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH


# Install Python dependencies
RUN apt-get update && apt-get install -y python3 python3-pip

# Create alias for 'python'
RUN ln -s /usr/bin/python3 /usr/bin/python  

#upgrade pip and install necessary packages
RUN pip3 install --upgrade pip
RUN pip3 install pyspark==3.1.2 py4j==0.10.9 cryptography pytest 


# Set working directory
WORKDIR /opt/spark/scripts

# Copy the transformation scripts
COPY . /opt/spark/scripts

# Command to execute the transformation
#CMD ["spark-submit", "/opt/spark/scripts/transformation.py"]



