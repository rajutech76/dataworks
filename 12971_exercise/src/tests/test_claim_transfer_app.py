import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

from src.claim_transfer_app import hash_claim_id, transform_data  # Assuming src/my_spark_app.py is in the same package


class TestContractClaimTransformation(unittest.TestCase):
  @classmethod
  def setUpClass(cls):
    cls.spark = SparkSession.builder.appName("Test Spark Session").getOrCreate()

  @classmethod
  def tearDownClass(cls):
    cls.spark.stop()
  
  def test_transform_data_success(self):
    """
    Tests successful transformation with valid data.
    """

    # Sample data (adjust schema and data as needed)
    contract_data = [
        ("Contract_SR_Europa_3", 408124123, "Direct", "01.01.2015", "01.01.2099", "17.01.2022 13:42"),
    ]
    claim_data = [
        ("Claim_SR_Europa_3", "CL_68545123", "Contract_SR_Europa_3", 97563756, 2, "14.02.2021", 523.21, "17.01.2022 14:45"),
    ]

    contract_df = self.spark.createDataFrame(contract_data,
                                              ["SOURCE_SYSTEM", "CONTRACT_ID", "CONTRACT_TYPE",
                                               "INSURED_PERIOD_FROM", "INSUDRED_PERIOD_TO", "CREATION_DATE"])
    claim_df = self.spark.createDataFrame(claim_data,
                                           ["SOURCE_SYSTEM", "CLAIM_ID", "CONTRACT_SOURCE_SYSTEM", "CONTRAT_ID", "CLAIM_TYPE",
                                            "DATE_OF_LOSS", "AMOUNT", "CREATION_DATE"])

    contract_path = "data/contract_test.csv"  # Replace with a temporary path for testing
    claim_path = "data/claim_test.csv"  # Replace with a temporary path for testing
    tf_path ="data/transfer_test.csv"

    contract_df.write.csv(contract_path, header=True)
    claim_df.write.csv(claim_path, header=True)

    # Call the function
    transaction_df = transform_data(self.spark, contract_path, claim_path,tf_path)

    # Assert expected schema and data
    expected_schema = StructType([
        StructField("CONTRACT_SOURCE_SYSTEM", StringType(), True),
        StructField("CONTRACT_SOURCE_SYSTEM_ID", IntegerType(), True),
        StructField("SOURCE_SYSTEM_ID", StringType(), True),
        StructField("TRANSACTION_TYPE", StringType(), True),
        StructField("TRANSACTION_DIRECTION", StringType(), True),
        StructField("CONFORMED_VALUE", DoubleType(), True),
        StructField("BUSINESS_DATE", TimestampType(), True),
        StructField("CREATION_DATE", TimestampType(), True),
        StructField("SYSTEM_TIMESTAMP", TimestampType(), True),
        StructField("NSE_ID", StringType(), True),
    ])
    self.assertEqual(transaction_df.schema, expected_schema)

    # Assert some expected data values (adjust as needed)
    expected_data = [
        ("Europe 3", 408124123, "68545123", "Corporate", "COINSURANCE", 523.21, "2021-02-14", "2022-01-17 14:45:00", None, ""),
    ]
    actual_data = transaction_df.select("CONTRACT_SOURCE_SYSTEM", "CONTRACT_SOURCE_SYSTEM_ID", "SOURCE_SYSTEM_ID",
                                         "TRANSACTION_TYPE", "TRANSACTION_DIRECTION", "CONFORMED_VALUE", "BUSINESS_DATE",
                                         "CREATION_DATE", "SYSTEM_TIMESTAMP", "NSE_ID").collect()
    self.assertEqual(expected_data, actual_data)

  def test_hash_claim_id(self):
    claim_id = "CL_123456"
    # Mock response (replace with actual API call during testing)
    response_json = {"Digest": "hashed_claim_id"}
    requests.get.return_value.json.return_value = response_json
    hashed_id = hash_claim_id(claim_id)
    self.assertEqual(hashed_id, "hashed_claim_id")
