from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, regexp_replace, to_date, current_timestamp, udf,when,contains
from pyspark.sql.types import StringType
import requests  # for external API call
from datetime import datetime

def hash_claim_id(claim_id):
  """
  Hashes a claim ID using the MD4 hash function via a REST API call.

  Args:
      claim_id (str): The claim ID to be hashed.

  Returns:
      str: The MD4 hash of the claim ID.
  """
  url = f"https://api.hashify.net/hash/md4/hex?value={claim_id}"
  response = requests.get(url)
  response.raise_for_status()  # Raise an exception for non-200 status codes
  data = response.json()
  nse_id=data.get("Digest")
  return nse_id

def dummy_nse_id(claim_id):
  return claim_id

def transform_data(spark, contract_path, claim_path,transform_path):
  """
  Transforms contract and claim data into transactions based on the specified mapping.

  Args:
      spark (SparkSession): Spark session object.
      contract_path (str): Path to the contract CSV file.
      claim_path (str): Path to the claim CSV file.

  Returns:
      DataFrame: The transformed DataFrame containing transactions.
  """
  # Read contract and claim data as DataFrames
  contracts_df = spark.read.csv(contract_path, header=True)
  claims_df = spark.read.csv(claim_path, header=True)
  claims_df =claims_df.withColumnRenamed("CREATION_DATE","CLAIM_CREATION_DATE")
  claims_df.show()
  
  contracts_df.show()
    
  # Define UDF for extracting claim ID without prefix
  get_claim_id = udf(lambda claim_id: regexp_extract(claim_id, r"\d+$", 1), StringType()) 
    
  # Define transformation logic
  transformed_df = contracts_df.join(
      claims_df.withColumn("SOURCE_SYSTEM_ID", get_claim_id(col("CLAIM_ID"))),
      on=col("CONTRACT_ID") == col("CONTRAT_ID"), how="left"
  ).withColumn("CONTRACT_SOURCE_SYSTEM", lit("Europe 3")) \
  .withColumn("CONTRACT_SOURCE_SYSTEM_ID", col("CONTRACT_ID")) \
  .withColumn(
    "TRANSACTION_TYPE",when(col("CLAIM_TYPE") == 1, "Private")
                     .when(col("CLAIM_TYPE") == 2, "Corporate")
                     .otherwise("Unknown")) \
  .withColumn(
      "TRANSACTION_DIRECTION",when(col("CLAIM_ID").contains("CL"), "COINSURANCE").
      when(col("CLAIM_ID").contains("RX"), "REINSURANCE")
                           
  ).withColumn("CONFORMED_VALUE", col("AMOUNT")) \
  .withColumn("BUSINESS_DATE", to_date(col("DATE_OF_LOSS"), "dd.MM.yyyy").alias("BUSINESS_DATE")) \
  .withColumn(
      "CREATION_DATE", to_date(col("CLAIM_CREATION_DATE"), "dd.MM.yyyy HH:mm:ss")
  ).withColumn("SYSTEM_TIMESTAMP", current_timestamp()) \
  .withColumn("NSE_ID", col("CLAIM_ID"))
  
  
  transformed_df.show() 

  # Select desired columns for the final DataFrame
  transformed_df = transformed_df.select(
      "CONTRACT_SOURCE_SYSTEM",
      "CONTRACT_SOURCE_SYSTEM_ID",
      "SOURCE_SYSTEM_ID",
      "TRANSACTION_TYPE",
      "TRANSACTION_DIRECTION",
      "CONFORMED_VALUE",
      "BUSINESS_DATE",
      "CREATION_DATE",
      "SYSTEM_TIMESTAMP",
      "NSE_ID",
  )
  #transformed_df.show()
  transformed_df.write.csv(transform_path, header=True)
  return transformed_df

# Example usage (optional)
if __name__ == "__main__":
  spark = SparkSession.builder.appName("Contract-Claim Transformation").getOrCreate()
  contract_path = "data/CONTRACT.csv"
  claim_path = "data/CLAIM.csv"
  transform_path= "data/out_data/TRANSACTIONS.csv"
  transform_data(spark, contract_path, claim_path,transform_path)
  spark.stop()
