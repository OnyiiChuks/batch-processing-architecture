""" Import needed packages for data ingestion"""
#import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import logging  # Import logging for better traceability
import os  # For environment variable handling
import sys
import time
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


"""
Initialize Spark session with needed configuration for spark master, spark worker, hadoop etc
 - Set application name
 - Configure HDFS as the default file system
 - Allocate memory for Spark driver and executor
 - Disable writing of _SUCCESS files after jobs
 - Optimize Parquet performance by disabling schema merging
"""
spark = SparkSession.builder \
    .appName("DataIngestion") \
    .config("spark.master", "local[*]") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://hdfs-namenode:9000") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
    .config("spark.sql.parquet.mergeSchema", "false") \
    .getOrCreate()



""" Paths to data for ingestion and storage
    input_path: path to raw data in the data folder
    output_path: path to where ingested data will be saved in hdfs
"""
input_path = "file:///app/ingestion/data/raw_fraud.csv"  
output_path = "hdfs://hdfs-namenode:9000/processed_fraud_data"


#Load encryption key from environment variable
SECRET_KEY = os.getenv("ENCRYPTION_KEY")
cipher_suite = Fernet(SECRET_KEY)


# Generate the key for encryption and decryption
def gen_key():
    key = Fernet.generate_key()
    print(f"Secret Key: {key.decode()}")


# Configurable parameters (which can be environment variables for flexibility)
BATCH_SIZE = int(os.getenv("BATCH_SIZE"))   # Number of rows per batch


#Function to encrypt data
def encrypt_data(data):
    if data is not None:
        return cipher_suite.encrypt(data.encode()).decode()
    return None

#Register encryption function as user defined function
encrypt_udf = udf(encrypt_data, StringType())



#Main function that performs the data ingestion processes
def ingest_fraud_data_in_batches(spark, input_path, output_path, batch_size, batch_number):    
    try:
        """
        Ingest data in batches and save each batch as a separate Parquet file in HDFS.
        """
        logger.info("Starting data ingestion process...")

        # Read the dataset
        df = spark.read.csv(input_path, header=True, inferSchema=True)

        #Count Number of rows in the dataset and divide into batches using the defined batch size
        total_rows = df.count()
        logger.info(f"Total rows in the dataset: {total_rows}")

        #Calculate the data chunk for this batch:
        start_index = (batch_number - 1) * batch_size
        #end_index = min(batch_number * batch_size, total_rows)
        end_index = __builtins__.min((batch_number + 1) * batch_size, total_rows)

        batch_df = df.limit(end_index).exceptAll(df.limit(start_index))
    
        # Drop the 'step' column
        batch_df = batch_df.drop("step")
        logger.info(f"The Column 'step' dropped")



        """ Encrypt sensitive columns like sender's account number or name(nameOrig) and 
                receiver's account number or name using the user defined function for encryption
        """
        #Register encryption function as user defined function
        encrypt_udf = udf(encrypt_data, StringType())
        
        logger.info("Data encryption starting...")
        batch_df = batch_df.withColumn("nameOrig", encrypt_udf(batch_df["nameOrig"]))
        batch_df = batch_df.withColumn("nameDest", encrypt_udf(batch_df["nameDest"]))
        logger.info("Data encryption done...")

            
        # Log schema and sample data for debugging
        logger.info(f"Schema of batch {batch_number}:")
        batch_df.printSchema()
        batch_df.show(5)


        #Write the current batch to the output path
        batch_output_path = f"{output_path}/batch_{batch_number}"
        batch_df.write.mode("overwrite").parquet(batch_output_path)
        logger.info(f"Batch {batch_number} saved to {batch_output_path}")
            
        if batch_number < 12:
                logger.info(f"Next batch comes up next month at the same time...")
                time.sleep(60)  #keep container running for few seconds, adjust as needed
               
        
    except Exception as e:
        logger.info(f"Error occurred: {e}")
    else:    
        logger.info(f"Data ingestion for batch {batch_number} completed.")
    finally:
        spark.stop()



#Run the app
if __name__ == "__main__":
    # Read the batch number from the command-line argument
    if len(sys.argv) < 2:
        raise ValueError("Batch number is required.")
    batch_number = int(sys.argv[1])
    logger.info(f"Running ingestion for batch {batch_number}")

    # Use batch_number to customize output
    ingest_fraud_data_in_batches(spark, input_path, output_path, BATCH_SIZE, batch_number=batch_number) 
