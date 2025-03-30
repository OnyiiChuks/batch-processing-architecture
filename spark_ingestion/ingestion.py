""" Import needed packages for data ingestion"""
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import math
import time
import logging  # Import logging for better traceability
import os  # For environment variable handling
from cryptography.fernet import Fernet  #for encryption
from pyspark.sql.types import StringType



# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# Initialize Spark session with needed configuration for spark master, spark worker, hadoop etc
spark = SparkSession.builder \
    .appName("DataIngestion") \
    .config("spark.master", "local[*]") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://hdfs-namenode:9000") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
    .config("spark.sql.parquet.mergeSchema", "false") \
    .getOrCreate()


# Paths to data for ingestion and storage
input_path = "file:///app/ingestion/data/raw_fraud.csv"  
output_path = "hdfs://hdfs-namenode:9000/processed_fraud_data"


#Load encryption key from environment variable
SECRET_KEY = os.getenv("ENCRYPTION_KEY")
cipher_suite = Fernet(SECRET_KEY)


# Generate a key
def gen_key():
    key = Fernet.generate_key()
    print(f"Secret Key: {key.decode()}")


# Configurable parameters (which can be environment variables for flexibility)
BATCH_SIZE = int(os.getenv("BATCH_SIZE"))   # Number of rows per batch
BATCH_INTERVAL_SECONDS = int(os.getenv("BATCH_INTERVAL_SECONDS")) 


#Function to encrypt data
def encrypt_data(data):
    if data is not None:
        return cipher_suite.encrypt(data.encode()).decode()
    return None

#Register encryption function as user defined function
encrypt_udf = udf(encrypt_data, StringType())



#Main function that performs the data ingestion processes
def ingest_fraud_data_in_batches(input_path, output_path, batch_size, interval_seconds):

    """
    Ingest data in batches and save each batch as a separate Parquet file in HDFS.
    """
    logger.info("Starting data ingestion process...")
 
    # Read the dataset
    df = spark.read.csv(input_path, header=True, inferSchema=True)


    # Drop the 'step' column
    df = df.drop("step")
    logger.info(f"The Column 'step' dropped")


    #Count Number of rows in the dataset and divide into batches using the defined batch size
    total_rows = df.count()
    logger.info(f"Total rows in the dataset: {total_rows}")
    
    num_batches = math.ceil(total_rows / batch_size)
    logger.info(f"Number of batches: {num_batches}")
    
    #iterate through the number of batches in the dataset to begin ingestion batch by batch
    for batch_number in range(num_batches):
        start_index = batch_number * batch_size
        #end_index = min((batch_number + 1) * batch_size, total_rows)
        # Python built-in min function is used, avoiding any conflicts with PySpark's min.
        end_index = __builtins__.min((batch_number + 1) * batch_size, total_rows)
        logger.info(f"Ingesting batch {batch_number + 1}/{num_batches}, rows {start_index} to {end_index}")
        
        # Filter rows for the current batch
        batch_df = df.limit(end_index).subtract(df.limit(start_index))


        """ Encrypt sensitive columns like sender's account number or name(nameOrig) and 
            receiver's account number or name using the user defined function for encryption
        """
        logger.info("Data encryption starting...")
        batch_df = batch_df.withColumn("nameOrig", encrypt_udf(batch_df["nameOrig"]))
        batch_df = batch_df.withColumn("nameDest", encrypt_udf(batch_df["nameDest"]))
        logger.info("Data encryption done...")

        
        # Log schema and sample data for debugging
        logger.info(f"Schema of batch {batch_number + 1}:")
        batch_df.printSchema()
        batch_df.show(5)


        #Write the current batch to the output path
        batch_output_path = f"{output_path}/batch_{batch_number + 1}"
        batch_df.write.parquet(batch_output_path, mode="overwrite")
        logger.info(f"Batch {batch_number + 1} saved to {batch_output_path}")
        

        """"
        if the batch number is less than the number of batches for the dataset, 
         wait before continuing with the ingestion of the next batch 
        """
        if batch_number < num_batches - 1:
            logger.info(f"Waiting for {interval_seconds} seconds before processing the next batch...")
            time.sleep(interval_seconds)
    
    logger.info("Data ingestion completed.")
    spark.stop()   


#Run the app
if __name__ == "__main__":
     ingest_fraud_data_in_batches(input_path, output_path, BATCH_SIZE, BATCH_INTERVAL_SECONDS)

# 2,592,000 seconds is 1 month
