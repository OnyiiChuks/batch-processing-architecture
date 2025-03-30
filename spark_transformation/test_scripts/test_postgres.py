import pytest
import os
from pyspark.sql import SparkSession
import subprocess

# Database Configurations
DB_USER = "your_db_user"
DB_PASSWORD = os.getenv("DB_PASSWORD")  # Ensure this is set in the environment
DB_HOST = "your-db-host"
DB_PORT = "5432"
DB_NAME = "your_db_name"
JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

DB_PROPERTIES = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "driver": "org.postgresql.Driver",
    "ssl": "true",
}

# Explicitly set JAVA_HOME inside your test
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"


# Pytest fixture to create a Spark session
@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder \
        .appName("TestSparkConnection") \
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
        query = "(SELECT 1) as test_query"
        df = spark.read.jdbc(url=JDBC_URL, table=query, properties=DB_PROPERTIES)
        assert df.count() == 1, "Database connection failed"
        print("Database connection successful!")
    except Exception as e:
        pytest.fail(f"Database connection test failed: {e}")

