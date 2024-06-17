from setuptools import setup, find_packages

setup(
    name="claim_transfer_app",
    version="0.1.0",
    description="12971 Exercise",
    packages=find_packages(where="src"),
    install_requires=[
        "pyspark",
        "pytest",  # Add pytest for unit testing
        "pytest-cov",# Python code coverage
        "requests"
    ],
    entry_points={
        "console_scripts": [
            "claim_tf_spark_app = src.claim_transfer_app:main"  # Entry point for running the app
        ]
    },
    #test_suite="src.tests",  # Specify the location of unit tests
)
