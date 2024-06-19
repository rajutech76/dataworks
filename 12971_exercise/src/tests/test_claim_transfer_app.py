import os
import pytest
from pyspark.sql import SparkSession
from src.claim_transfer_app import hash_claim_id,  write_to_csv, transform_data
from unittest.mock import patch
from unittest import mock

@pytest.fixture(scope="session", autouse=True)
def spark_session():
    spark = SparkSession.builder.appName("pytest").getOrCreate()
    yield spark
    spark.stop()


# Mock for invoke_rest_endpoint function
@pytest.fixture
def mock_invoke_rest(mocker):
    return mocker.patch('src.claim_transfer_app.hash_claim_id')

def test_hash_claim_id(mock_invoke_rest):
    # Set the return value of the mocked function
    mock_invoke_rest.return_value = "123"

    # Call the function under test
    result = hash_claim_id("claim_id")

    # Assert that the result is the expected mocked value
    assert result == "123"

    
def test_write_to_csv(spark_session):
    df = spark_session.createDataFrame([("123", "abc")], ["id", "value"])
    output_dir = "test_data"
    write_to_csv(df, output_dir)

    # Read the CSV file back into a DataFrame
    csv_file = None
    for file in os.listdir(output_dir):
        if file.endswith(".csv"):
            csv_file = os.path.join(output_dir, file)
            break

    assert csv_file is not None, "CSV file not found in the output directory"

    read_df = spark_session.read.csv(csv_file, header=True)

    # Compare the original DataFrame with the read DataFrame
    assert df.subtract(read_df).count() == 0, "DataFrames do not match"
    assert read_df.subtract(df).count() == 0, "DataFrames do not match"

    # Clean up the test output directory
    os.remove(csv_file)

def test_transform_data(spark_session):
    # Create mock data for contracts_df and claims_df
    result_df = transform_data(spark_session, "test_data/test_contract.csv", "test_data/test_claim.csv", "test_data/test_out")
    # Add assertions to check if the transformation is correct
