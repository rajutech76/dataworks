# PySpark End-to-End Application Development (Windows Local Environment)

This repository provides a hands-on introduction to developing and packaging end-to-end PySpark applications specifically tailored for local execution on Windows machines (laptops, desktops).It serves as a comprehensive learning resource for anyone interested in getting started with PySpark in this environment.

## Use Case Overview

This project demonstrates how to transform two sample datasets, `contract.csv` and `claim.csv`, based on the `claim_id` column,
resulting in a new output file named `transform.csv`.

## Prerequisites

To run the code samples in this repository, you'll need the following prerequisites:

* Python (version 3.12.1 or higher)
* Apache Spark (version 3.5.1 or higher)
* pyspark (version 3.5.1 or higher)


## Getting Started 

1. Clone this repository: `git clone https://github.com/rajutech76/dataworks.git`
2. Navigate to the project directory: `cd dataworks-main/12971_exercise`
3. Install project dependencies 
	pip install -e . (This will install the dependecies mentioned in "12971_exercise/setup.py")
	
4. Run the Testcase	

	pytest src/tests
	
5.Execute the Test Coverage 

	pytest --cov=src --cov-report=term

6.Run the pyspark application locally 
	
	python main_local.py
	
7.Submiting the pyspark application to SparkCluster

  spark-submit main_local.py \
  --master local[n] \  # Adjust n for parallelism (e.g., local[2] for 2 cores)
	

## Troubleshooting

     TBD

## Additional Resources

* Apache Spark documentation: https://spark.apache.org/documentation.html
* Installing Pyspark on Windows: https://medium.com/@deepaksrawat1906/a-step-by-step-guide-to-installing-pyspark-on-windows-3589f0139a30
