from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

class DataReader:
    def __init__(self, spark: SparkSession):
        """
        Initialize the DataReader with an existing Spark session.

        Parameters:
            spark (SparkSession): An active Spark session.
        """
        self.spark = spark

    def read_parquet(self, path: str) -> DataFrame:
        """
        Reads a Parquet file and returns a DataFrame.

        Parameters:
            path (str): The path to the Parquet file.

        Returns:
            DataFrame: A Spark DataFrame containing the data from the Parquet file.
        """
        accounts_df = self.spark.read.option("header", "true").parquet(path)
        return accounts_df

    def read_json(self, path: str,telemetry_schema: StructType) -> DataFrame:
        """
        Reads JSON files from the specified path and returns a DataFrame.

        Parameters:
            path (str): The path to the JSON files (can include wildcards).

        Returns:
            DataFrame: A Spark DataFrame containing the data from the JSON files.
        """
        json_df = self.spark.read.schema(telemetry_schema).json(path)
        return json_df