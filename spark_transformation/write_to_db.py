
"""Connecting and saving data to postgreSql"""

#Import needed packages
import logging
import os


# Fetching user and password from environment variables
DB_USER = os.getenv("DB_USER")  # Default to `db_user`   
DB_PASSWORD = os.getenv("DB_PASSWORD")


# Database configuration
DB_HOST = "postgres"  # Docker service name from docker-compose.yml
DB_PORT = 5432
DB_NAME = "my_database"

# Configure logging format
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Function to write to the PostgreSQL database
def write_to_database(dataframe, table_name):
    """
    Write transformed data to the PostgreSQL database using JDBC.

    Args:
    dataframe (DataFrame): Spark DataFrame to write.
    table_name (str): Target table name in the PostgreSQL database.
    """
    try:
        # JDBC connection URL
        jdbc_url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}?ssl=true&sslmode=require&sslfactory=org.postgresql.ssl.NonValidatingFactory"
        
        #logging.info(f"The jdbc url is {jdbc_url}") #for debugging
        
        # JDBC properties
        db_properties = {
            "user": DB_USER,
            "password": os.getenv("DB_PASSWORD"),  
            "driver": "org.postgresql.Driver",
            "ssl": "true"
        }
        #logging.info(f"The jdbc properties are: {db_properties}") #for debugging

        # Write the Spark DataFrame to the PostgreSQL database
        logging.info(f"Writing data to the table '{table_name}' in the database...")
        
        dataframe.write\
            .jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=db_properties)

        logging.info(f"Data successfully written to the table '{table_name}' in the database.")
    
    except Exception as e:
        logging.error(f"An error occurred while writing to the database: {e}")
