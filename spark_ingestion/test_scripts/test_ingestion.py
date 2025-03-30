import os
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
#from ingestion import ingest_data_in_batches_fraud
from dotenv import load_dotenv


# Explicitly load the .env file
load_dotenv()

# Manually set SECRET_KEY if it's not available
#if "SECRET_KEY" not in os.environ:
    #os.environ["SECRET_KEY"] = "bNDz-TryHVHsdiZWkFTh4p1V_3NUQBNW5KaABzdwqbM="

# Verify if SECRET_KEY is loaded
print("SECRET_KEY:", os.getenv("SECRET_KEY"))  # Debugging

from ingestion import ingest_fraud_data_in_batches

# Set environment variables
os.environ["JAVA_HOME"] = "/usr/local/openjdk-11"
os.environ["SPARK_HOME"] = "/opt/spark"

@pytest.fixture(scope="session")
def spark():
    """Create a single Spark session for all tests."""
    spark = SparkSession.builder \
        .appName("TestIngestion") \
        .config("spark.master", "local[*]") \
        .config("spark.hadoop.fs.defaultFS", "file:///")\
        .config("spark.sql.execution.pythonUDF.arrow.enabled", "false") \
        .getOrCreate()
    
    yield spark
    spark.stop()
#.config("spark.hadoop.fs.defaultFS", "hdfs://hdfs-namenode:9000") \
#.config("spark.hadoop.fs.defaultFS", "file:///")\
@pytest.fixture
def sample_data(spark):
    """Create sample DataFrame."""
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ])
    data = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
    return spark.createDataFrame(data, schema)

'''def test_ingestion(spark):
    """Test ingestion function."""
    print("Running ingestion test...")

    input_path = "file:///app/fake/input.csv"  #"/fake/input.csv"
    output_path = "file:///app/fake/output"   #"/fake/output"
    batch_size = 2
    interval_seconds = 1

    try:
        ingest_fraud_data_in_batches(spark, input_path, output_path, batch_size, interval_seconds)
        print("Test Passed: Ingestion function executed successfully.")
    except Exception as e:
        pytest.fail(f"Test Failed: {e}")
'''


def test_ingestion(spark, tmp_path):
    """Test ingestion function."""
    print("Running ingestion test...")

    input_path = str(tmp_path / "input.csv")
    output_path = str(tmp_path / "output")

    # Create a fake CSV file
    with open(input_path, "w") as f:
        f.write("id,name\n1,Alice\n2,Bob\n")

    batch_size = 2
    interval_seconds = 1

    try:
        ingest_fraud_data_in_batches(spark, input_path, output_path, batch_size, interval_seconds)
        print("Test Passed: Ingestion function executed successfully.")
    except Exception as e:
        pytest.fail(f"Test Failed: {e}")