
'''A pytest to test that transformation.py successfully inserted data into PostgreSQL.
   The test must run in the spark-transformation container while the container is still running:
    Enter into the container: " docker exec -it spark-transformation bash "
    then run the test: python3 -m pytest test_scripts/test_postgres.py
'''

import pytest
import os
from pyspark.sql import SparkSession
#import subprocess


# Database Configurations
DB_USER = os.getenv("DB_USER") #set in the environment
DB_PASSWORD = os.getenv("DB_PASSWORD")  # set in the environment
DB_HOST = "postgres"  # Docker service name from docker-compose.yml
DB_PORT = 5432
DB_NAME = "my_database"
JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}?ssl=true&sslmode=require&sslfactory=org.postgresql.ssl.NonValidatingFactory"


# JDBC properties
DB_PROPERTIES = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "driver": "org.postgresql.Driver",
    "ssl": "true",
}


# Pytest fixture to create a Spark session
@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder \
        .appName("TestSparkPostgresConnection") \
        .master("local[*]") \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.2.5.jar").getOrCreate() 
    yield spark
    spark.stop()


    
def test_db_connection(spark):
    """
    Test if the service can successfully connect to the PostgreSQL database using
      Spark's JDBC connector.
    """
    try:
        query = "(SELECT 1 AS test_column) As test_query"
        df = spark.read.jdbc(url=JDBC_URL, table=query, properties=DB_PROPERTIES)
        print("Query executed. Rows returned:", df.count())
        assert df.count() == 1, "Database connection failed"
        print("Database connection successful!")
    except Exception as e:
        pytest.fail(f"Database connection test failed: {e}")

