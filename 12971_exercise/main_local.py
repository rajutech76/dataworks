from src.claim_transfer_app import transform_data
import os
import sys
from pyspark import SparkContext
from pyspark.conf import SparkConf 
from pyspark.sql import SparkSession 
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

contract_path = "data/contract.csv"
claim_path = "data/claim.csv"
transform_path= "data/out_data/TRANSACTIONS.csv"
spark = SparkSession.builder.appName("12971_exercise").config("spark.executor.memory", "4g").getOrCreate()

tf_df = transform_data(spark=spark,
                       contract_path=contract_path,
                       claim_path=claim_path,
                       transform_path=transform_path)
#tf_df.show()