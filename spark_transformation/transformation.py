""" Import needed packages for the data transformation."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *  
from cryptography.fernet import Fernet
#from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from write_to_db import write_to_database
import os
import sys
import logging
import time



# Set logging formate which will be sent to both a file and in the console
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


""" Initialize Spark session with custom configurations for Docker environment:
# - Set application name
# - Include PostgreSQL JDBC driver for database connectivity
# - Configure HDFS as the default file system
# - Allocate memory for Spark driver and executor
# - Disable writing of _SUCCESS files after jobs
# - Optimize Parquet performance by disabling schema merging
"""
spark = SparkSession.builder \
    .appName("DataTransformation") \
    .config("spark.jars", "/opt/spark/jars/postgresql-42.2.5.jar") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://hdfs-namenode:9000") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
    .config("spark.sql.parquet.mergeSchema", "false") \
    .getOrCreate()


# Load decryption key from .env file (the same as encryption key)
SECRET_KEY = os.getenv("ENCRYPTION_KEY")  
cipher_suite = Fernet(SECRET_KEY)


""" input_path - path to the ingested data in hdfs
    output_path - path tostore the transformed data to hdfs if needed
    table_name - table name where transformed data can be stored in PostgreSQL. 
"""
input_path = "hdfs://hdfs-namenode:9000/processed_fraud_data"
output_path = "hdfs://hdfs-namenode:9000/transformed_fraud_data"
table_name = "transformed_data"



"""Function to decrypt data"""
def decrypt_data(data):
    if data is not None:
        return cipher_suite.decrypt(data.encode()).decode()
    return None

#register the decrytion function as user defined function
decrypt_udf = udf(decrypt_data, StringType())



"""function to save transformed data back to hdfs.
   this can be usefully if further transformation on the processed data is needed 
   Output_path - the path where the transformed data will be saved in hdfs
"""
def save_data_to_hdfs(df, output_path):
    #Save as parquet data

    logger.info("Saving transformed data back to hdfs")
    df.write.parquet(output_path, mode="overwrite")
    logger.info("Parquet data saved to hdfs")




"""Function that performs the data transformation processes
   input_path - path to the data in hdfs
   table_name - name of the table from postgreSQL where the transformed data will be stored.
"""
def perform_transformation(input_path,table_name, batch_num):   #output_path,
    try:
        #transform the data according to the batches the were saved

            batch = f"{input_path}/batch_{batch_num}"
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
            logger.info("============================================================================================================")
            
            # Drop duplicates records
            df = df.dropDuplicates()

            # Drop rows where 'isFraud' column has null values
            df = df.dropna(subset=["isFraud"])

            # Rename the 'type' column to 'transtype' ie transaction type
            df = df.withColumnRenamed("type", "transType")


            #--------------------------------------------------------------------------------------------------------------------------
            #Data Aggregation
            #--------------------------------------------------------------------------------------------------------------------------
            
            logger.info(f"Starting Data Aggragation ...")
            logger.info("=============================================================================================================")
            
            
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

            fraud_by_type.show()
            


            '''
            Average amount for fraudulent transactions and Maximum fraud amount recorded
            Helps determine the usual size of fraudulent transactions and the largest fraud cases.
            '''
            fraud_stats = df.filter(col("isFraud") == 1).agg(avg("amount").alias("avg_fraud_amount"),
            max("amount").alias("max_fraud_amount")
            )

            fraud_stats.show()
            
            

            '''Total flagged fraud transactions (isFlaggedFraud == 1) and Percentage of flagged fraud that are actually fraud (isFlaggedFraud == 1 & isFraud == 1)
               This measures how accurate flagged transactions are in detecting real fraud.
            '''
            flagged_fraud_stats = df.select(
            sum(col("isFlaggedFraud").cast("bigint")).alias("total_flagged"),
            sum((col("isFlaggedFraud").cast("bigint") * col("isFraud").cast("bigint"))).alias("true_positive_flags")
            ).withColumn(
            "flag_accuracy", (col("true_positive_flags") / col("total_flagged")) * 100
             )
            
            flagged_fraud_stats.show()
        


            # Write aggregated data to PostgreSQL
            logger.info("Writing aggregated data to the PostgreSQL database...")
            
            write_to_database(fraud_by_type, "fraud_by_type")
            logger.info(f"Data successfully written to the fraud_by_type table in the database.")

            write_to_database(fraud_stats,"fraud_stats")
            logger.info(f"Data successfully written to the fraud_stats table in the database.")

            write_to_database(flagged_fraud_stats, "flagged_fraud_accuracy") 
            logger.info(f"Data successfully written to the flagged_fraud_stats table in the database.")

            

            #---------------------------------------------------------------------------------------------------------------------
            # Schema Validation:
            #---------------------------------------------------------------------------------------------------------------------
            
            logger.info("Starting Schema validation...")
            logger.info("==========================================================================================================")

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


            #Save transformed data back to hdfs (if needed)
            #save_data_to_hdfs(df, output_path)

            time.sleep(900) # wait for few minutes before stopping the container, enough time to run the pytest.
        

    except Exception as e:
        logger.info(f"Error occurred: {e}")
    else:    
        logger.info(f"Data transformation for batch {batch_num} completed. Expecting the next scheduled batch...")
    finally:
        spark.stop()



if __name__ == "__main__":
    # Read the batch number from the command-line argument
    if len(sys.argv) < 2:
        raise ValueError("Batch number is required.")
    batch_number = int(sys.argv[1])
    logger.info(f"Running transformation for batch {batch_number}")

    #perform transformation using batch_number
    perform_transformation(input_path,table_name,batch_number)

