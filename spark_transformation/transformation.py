""" Import needed packages for the data transformation."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *  
from cryptography.fernet import Fernet
#from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from write_to_db import write_to_database
import time
import os
import logging



# Set logging formate which will be sent to both a file and in the console
logging.basicConfig(
    #filename="/opt/spark/logs/spark_transform.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# Initialize Spark session for docker1
spark = SparkSession.builder \
    .appName("DataTransformation") \
    .config("spark.jars", "/opt/spark/jars/postgresql-42.2.5.jar") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://hdfs-namenode:9000") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
    .config("spark.sql.parquet.mergeSchema", "false") \
    .getOrCreate()



# Path to the ingested fraud data from hdfs
input_path = "hdfs://hdfs-namenode:9000/processed_fraud_data"

# Table name in PostgreSQL
table_name = "transformed_data"

#set time interval between each ingestion
interval_seconds = int(os.getenv("BATCH_INTERVAL_SECONDS"))

# Load decryption key (the same as encryption key)
SECRET_KEY = os.getenv("ENCRYPTION_KEY")  
cipher_suite = Fernet(SECRET_KEY)



"""Function to decrypt the data"""
def decrypt_data(data):
    if data is not None:
        return cipher_suite.decrypt(data.encode()).decode()
    return None

#register the decrytion function as user defined function
decrypt_udf = udf(decrypt_data, StringType())



"""Function that performs the data transformation processes
   input_path - path to the data in hdfs
   table_name - name of the table from postgreSQL where the transformed data will be stored.
"""
def perform_transformation(input_path,table_name):   
    try:
        #transform the data according to the batches the were saved
        batches = [f"{input_path}/batch_{i}" for i in range(1, 22)] 

        for batch in batches:
            logger.info(f"Processing {batch}")
            
            # Read the parquet file in batches
            df = spark.read.parquet(batch)

            # Log schema and sample data for debuging
            logger.info(f"Schema of batch {batch}:")
            df.printSchema()
            df.show(5)


            """Decrypt the encrypted sensitive columns using the user defined function
            """
            logger.info("Data decryption starting...")
            df = df.withColumn("nameOrig", decrypt_udf(df["nameOrig"]))
            df = df.withColumn("nameDest", decrypt_udf(df["nameDest"]))
            logger.info("Data decryption Done.")


            #-------------------------------------------------------------------------------------------------------------------------
            # Data Cleaning
            #-------------------------------------------------------------------------------------------------------------------------
            
            logger.info("Starting Data cleaning...")
            logger.info("=====================================================================================================================")
            
            # Drop duplicates records
            df = df.dropDuplicates()

            # Drop rows where 'isFraud' column has null values
            df = df.dropna(subset=["isFraud"])

            # Rename the 'type' column to 'acctype'
            df = df.withColumnRenamed("type", "transType")


            #----------------------------------------------------------------------------------------------------------
            #Data Aggregation
            #-----------------------------------------------------------------------------------------------------------
            
            logger.info(f"Starting Data Aggragation ...")
            logger.info("===============================================================================================================")
            
            
            #make sure Spark treats isFraud and isFlaggedFraud as boolean by using 'cast'
            df = df.withColumn("isFraud", col("isFraud").cast("boolean"))
            df = df.withColumn("isFlaggedFraud", col("isFlaggedFraud").cast("boolean"))

            

            '''Aggregate Fraud by Transaction Type, Count of fraudulent transactions per type and Percentage of fraud per transaction type
               This will help to shows which transaction types are most affected by fraud.
            '''
            fraud_by_type = df.groupBy("transType").agg(count("*").alias("total_transactions"),
            sum(col("isFraud").cast("int")).alias("fraudulent_transactions")
            ).withColumn(
            "fraud_rate", (col("fraudulent_transactions") / col("total_transactions")) * 100
            )
            

            '''
            Average amount for fraudulent transactions and Maximum fraud amount recorded
            Helps determine the usual size of fraudulent transactions and the largest fraud cases.
            '''
            fraud_stats = df.filter(col("isFraud") == 1).agg(avg("amount").alias("avg_fraud_amount"),
            max("amount").alias("max_fraud_amount")
        )
            

            '''Total flagged fraud transactions (isFlaggedFraud == 1) and Percentage of flagged fraud that are actually fraud (isFlaggedFraud == 1 & isFraud == 1)
               This measures how accurate flagged transactions are in detecting real fraud.
            '''
            flagged_fraud_stats = df.select(
            sum(col("isFlaggedFraud").cast("bigint")).alias("total_flagged"),
            sum((col("isFlaggedFraud").cast("bigint") * col("isFraud").cast("bigint"))).alias("true_positive_flags")
            ).withColumn(
            "flag_accuracy", (col("true_positive_flags") / col("total_flagged")) * 100
             )
             
             

            # Write aggregated data to PostgreSQL
            logger.info("Writing aggregated data to the PostgreSQL database...")
            
            write_to_database(fraud_by_type, "fraud_by_type")
            logger.info(f"Data successfully written to the fraud_by_type table in the database.")

            write_to_database(fraud_stats,"fraud_stats")
            logger.info(f"Data successfully written to the fraud_stats table in the database.")

            write_to_database(flagged_fraud_stats, "flagged_fraud_accuracy") 
            logger.info(f"Data successfully written to the flagged_fraud_stats table in the database.")

            

            #---------------------------------------------------------------------------------------------------
            # Schema Validation:
            #----------------------------------------------------------------------------------------------------
            
            logger.info("Starting Schema validation...")
            logger.info("===================================================================================================")

            #Ensure the dataframe has the expected columns  
            expected_columns = ["transactionID", "transType" , "amount", "nameOrig", "oldbalanceOrg", "newbalanceOrig",\
                       "nameDest","oldbalanceDest", "newbalanceDest","isFraud", "isFlaggedFraud", "timestamp"]
            

            if set(expected_columns) != set(df.columns):
                    raise ValueError("Schema validation failed! Ensure the dataset has the correct columns.")
            else:
                logger.info("Schema validation successful...")


            #Print schema and preview data for debugging
            logger.info("DataFrame Schema:")
            df.printSchema()
            logger.info("DataFrame Preview:")
            df.show(5)


            # Write transformed data to PostgreSQL
            logger.info("Writing transformed data to the PostgreSQL database...")
            write_to_database(df, table_name)
            logger.info(f"Data successfully written to the {table_name} table in the database.")


            #Save transformed data to hdfs
            #Save directly to parquet
            ##df.write.parquet(output_path, mode="overwrite")
            ##logger.info("Parquet data saved")

            
            logger.info(f"Waiting for {interval_seconds} seconds before transforming the next batch...")
            time.sleep(interval_seconds)


    except Exception as e:
        logger.info(f"Error occurred: {e}")
    else:    
        logger.info("Data transformation completed.")
    finally:
        spark.stop()


if __name__ == "__main__":
    #Save transformed data to postgres 
    perform_transformation(input_path,table_name)

    #save transformed data back to hdfs
    #perform_transformation(input_path,output_path)

