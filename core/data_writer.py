from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

class DataWriter:
    def __init__(self, spark: SparkSession):
        """
        Initialize the DataReader with an existing Spark session.

        Parameters:
            spark (SparkSession): An active Spark session.
        """
        self.spark = spark

    def write_parquet(self, aggregated_df: DataFrame):
        """
        Write a multiple Parquet files partitioned by Customer Name, Installation Id, Version and Date.

        Parameters:
            aggregated_df (DataFrame): The Aggregated data which generates the report in parquet format.

        Returns:
            Do not return anything.
        """

        # Save the final DataFrame to partitioned Parquet files
        # aggregated_df.write.partitionBy("customer_name", "id_installation", "version","date").parquet("/opt/spark-apps/output/processed_telemetry_data.parquet")
        aggregated_df.write.mode("overwrite").parquet("/opt/spark-apps/output/")