'''Modified to rmove pyarrow'''

import pytest
import fsspec
import pyarrow

# HDFS Configuration
HDFS_URL = "hdfs://your-hdfs-host:9000/"  # Change to your actual HDFS NameNode URL

@pytest.fixture(scope="module")
def hdfs_client():
    """
    Fixture to establish an HDFS connection using fsspec.
    """
    try:
        hdfs = fsspec.filesystem("hdfs", use_ssl=False)
        yield hdfs
    except Exception as e:
        pytest.fail(f"Failed to connect to HDFS: {e}")

def test_hdfs_connection(hdfs_client):
    """
    Test if the service can successfully connect to HDFS.
    """
    assert hdfs_client.exists(HDFS_URL), "HDFS connection failed"
    print("HDFS connection successful!")

def test_hdfs_list_directory(hdfs_client):
    """
    Test if listing HDFS root directory works.
    """
    files = hdfs_client.ls(HDFS_URL)
    assert isinstance(files, list), "Failed to list directory in HDFS"
    print(f"HDFS files: {files}")