
'''A pytest to test that ingestion.py successfully inserted data into hdfs.
   The test should run in the spark-transformtion container while the container is still running.
   Enter into the container: " docker exec -it spark-transformation bash "
   then run the test: python3 -m pytest test_scripts/test_hdfs.py
'''
import pytest
from pyspark.sql import SparkSession

#path to the processed data stored in hdfs
HDFS_URL = "hdfs://hdfs-namenode:9000/processed_fraud_data/"


@pytest.fixture(scope="module")
def spark():
    """Initialize Spark session for tests."""
    return SparkSession.builder.master("local[*]").appName("HDFS_Test").getOrCreate()


def test_hdfs_connection(spark):
    try:
        # Pick the first available batch
        df = spark.read.parquet(HDFS_URL + "batch_1")
        assert df.count() >= 0  # Just to confirm it reads something
        print("HDFS Connection Successful!")
    except Exception as e:
        assert False, f"HDFS connection failed: {e}"


